#!/usr/bin/env python3
"""Convert a PEFT LoRA safetensors file to LiteRT-LM external-buffer TFLite.

LiteRT-LM maps only the first MiB of a LoRA file as FlatBuffer metadata. Tensor
payloads therefore must use TFLite Buffer.offset/size records and live outside
that metadata region. A conventional TFLite file with inline Buffer.data values
can parse when read in full but is not a valid LiteRT-LM LoRA artifact.

This is an offline build tool. The deployed Grok binaries do not depend on
Python, NumPy, safetensors, flatbuffers, or the generated tflite Python schema.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import flatbuffers
import numpy as np
import safetensors
import tflite


METADATA_REGION_BYTES = 1024 * 1024
PAYLOAD_ALIGNMENT_BYTES = 16 * 1024
FIRST_PAYLOAD_OFFSET = 64 * 1024

PROJECTIONS = {
    "q_proj": "q",
    "k_proj": "k",
    "v_proj": "v",
    "o_proj": "o",
}
SUPPORTED_TARGETS = tuple(f"self_attn.{projection}" for projection in PROJECTIONS)


@dataclass(frozen=True)
class TensorPayload:
    name: str
    values: np.ndarray
    offset: int


def align(value: int, alignment: int) -> int:
    return (value + alignment - 1) // alignment * alignment


def tensor_name(projection: str, side: str, layer: int, naming: str) -> str:
    short = PROJECTIONS[projection]
    if naming == "compiled":
        return f"lora_atten_{short}_{side}_prime_weight_{layer}"
    prefixes = {"q": "query", "k": "key", "v": "value", "o": "post"}
    direction = "left" if side == "a" else "right"
    return f"{prefixes[short]}_w_prime_{direction}_{layer}"


def expected_shape(
    projection: str,
    side: str,
    rank: int,
    hidden_size: int,
    kv_size: int,
) -> tuple[int, int]:
    if side == "a":
        return (hidden_size, rank)
    output_size = kv_size if projection in {"k_proj", "v_proj"} else hidden_size
    return (rank, output_size)


def load_attention_tensors(
    safetensors_path: Path,
    *,
    layers: int,
    rank: int,
    hidden_size: int,
    kv_size: int,
    scale: float,
    dtype: str,
    naming: str,
    declared_targets: list[str],
    allow_partial_attention: bool,
) -> tuple[list[tuple[str, np.ndarray]], list[str], int]:
    destination_dtype = np.float32 if dtype == "f32" else np.float16
    expected_sources: list[tuple[str, str, str, int]] = []
    for layer in range(layers):
        for projection in PROJECTIONS:
            for side in ("a", "b"):
                source = f"model.layers.{layer}.self_attn.{projection}.lora_{side}"
                expected_sources.append((source, projection, side, layer))

    converted: list[tuple[str, np.ndarray]] = []
    with safetensors.safe_open(str(safetensors_path), framework="np") as source:
        available = set(source.keys())
        expected_declared = {
            f"model.layers.{layer}.{target}.lora_{side}"
            for layer in range(layers)
            for target in declared_targets
            for side in ("a", "b")
        }
        missing_declared = sorted(expected_declared - available)
        undeclared = sorted(available - expected_declared)
        if missing_declared or undeclared:
            missing_preview = ", ".join(missing_declared[:4]) or "none"
            undeclared_preview = ", ".join(undeclared[:4]) or "none"
            raise ValueError(
                "adapter tensors do not match lora_parameters.keys: "
                f"missing={len(missing_declared)} ({missing_preview}); "
                f"undeclared={len(undeclared)} ({undeclared_preview})"
            )
        unsupported_targets = sorted(set(declared_targets) - set(SUPPORTED_TARGETS))
        if unsupported_targets and not allow_partial_attention:
            raise ValueError(
                "the compiled LiteRT-LM artifact exposes attention LoRA inputs only, "
                "but this adapter also targets "
                f"{', '.join(unsupported_targets)}; pass --allow-partial-attention "
                "to explicitly omit those tensors"
            )
        missing = [name for name, _, _, _ in expected_sources if name not in available]
        if missing:
            preview = ", ".join(missing[:8])
            raise ValueError(
                f"adapter is missing {len(missing)} required attention tensors: {preview}"
            )
        for source_name, projection, side, layer in expected_sources:
            values = source.get_tensor(source_name)
            shape = expected_shape(projection, side, rank, hidden_size, kv_size)
            if tuple(values.shape) != shape:
                raise ValueError(
                    f"{source_name} has shape {tuple(values.shape)}; expected {shape}"
                )
            # MLX-LM applies `scale * ((x @ A) @ B)` at inference time. The
            # LiteRT compiled signature has no per-adapter scale input, so
            # fold the training scale into B while preserving A verbatim.
            if side == "b":
                values = np.asarray(values, dtype=np.float32) * scale
            values = np.ascontiguousarray(values, dtype=destination_dtype)
            converted.append((tensor_name(projection, side, layer, naming), values))
    ignored_tensor_count = len(available) - len(converted)
    return converted, unsupported_targets, ignored_tensor_count


def create_buffers(
    builder: flatbuffers.Builder, payloads: list[TensorPayload]
) -> int:
    offsets: list[int] = []
    tflite.BufferStart(builder)
    offsets.append(tflite.BufferEnd(builder))
    for payload in payloads:
        tflite.BufferStart(builder)
        tflite.BufferAddOffset(builder, payload.offset)
        tflite.BufferAddSize(builder, payload.values.nbytes)
        offsets.append(tflite.BufferEnd(builder))
    tflite.ModelStartBuffersVector(builder, len(offsets))
    for offset in reversed(offsets):
        builder.PrependUOffsetTRelative(offset)
    return builder.EndVector()


def create_tensors(
    builder: flatbuffers.Builder, payloads: list[TensorPayload], dtype: str
) -> int:
    tensor_type = tflite.TensorType.FLOAT32 if dtype == "f32" else tflite.TensorType.FLOAT16
    offsets: list[int] = []
    for buffer_index, payload in enumerate(payloads, start=1):
        name = builder.CreateString(payload.name)
        shape = tuple(int(value) for value in payload.values.shape)
        tflite.TensorStartShapeVector(builder, len(shape))
        for dimension in reversed(shape):
            builder.PrependInt32(dimension)
        shape_vector = builder.EndVector()
        tflite.TensorStart(builder)
        tflite.TensorAddShape(builder, shape_vector)
        tflite.TensorAddType(builder, tensor_type)
        tflite.TensorAddBuffer(builder, buffer_index)
        tflite.TensorAddName(builder, name)
        offsets.append(tflite.TensorEnd(builder))
    tflite.SubGraphStartTensorsVector(builder, len(offsets))
    for offset in reversed(offsets):
        builder.PrependUOffsetTRelative(offset)
    return builder.EndVector()


def empty_int_vector(builder: flatbuffers.Builder) -> int:
    builder.StartVector(4, 0, 4)
    return builder.EndVector()


def create_subgraph(builder: flatbuffers.Builder, tensors: int) -> int:
    inputs = empty_int_vector(builder)
    outputs = empty_int_vector(builder)
    tflite.SubGraphStartOperatorsVector(builder, 0)
    operators = builder.EndVector()
    name = builder.CreateString("lora")
    tflite.SubGraphStart(builder)
    tflite.SubGraphAddTensors(builder, tensors)
    tflite.SubGraphAddInputs(builder, inputs)
    tflite.SubGraphAddOutputs(builder, outputs)
    tflite.SubGraphAddOperators(builder, operators)
    tflite.SubGraphAddName(builder, name)
    subgraph = tflite.SubGraphEnd(builder)
    tflite.ModelStartSubgraphsVector(builder, 1)
    builder.PrependUOffsetTRelative(subgraph)
    return builder.EndVector()


def create_metadata(builder: flatbuffers.Builder, rank: int) -> int:
    name = builder.CreateString("lora_rank")
    tflite.MetadataStart(builder)
    tflite.MetadataAddName(builder, name)
    # LiteRT-LM intentionally interprets Metadata.buffer as the rank value.
    tflite.MetadataAddBuffer(builder, rank)
    metadata = tflite.MetadataEnd(builder)
    tflite.ModelStartMetadataVector(builder, 1)
    builder.PrependUOffsetTRelative(metadata)
    return builder.EndVector()


def build_metadata(payloads: list[TensorPayload], rank: int, dtype: str) -> bytes:
    builder = flatbuffers.Builder(128 * 1024)
    buffers = create_buffers(builder, payloads)
    tensors = create_tensors(builder, payloads, dtype)
    subgraphs = create_subgraph(builder, tensors)
    metadata = create_metadata(builder, rank)
    tflite.ModelStartOperatorCodesVector(builder, 0)
    operator_codes = builder.EndVector()
    description = builder.CreateString("Grok LiteRT-LM external-buffer LoRA")
    tflite.ModelStart(builder)
    tflite.ModelAddVersion(builder, 3)
    tflite.ModelAddOperatorCodes(builder, operator_codes)
    tflite.ModelAddSubgraphs(builder, subgraphs)
    tflite.ModelAddDescription(builder, description)
    tflite.ModelAddBuffers(builder, buffers)
    tflite.ModelAddMetadata(builder, metadata)
    model = tflite.ModelEnd(builder)
    builder.Finish(model, file_identifier=b"TFL3")
    return bytes(builder.Output())


def materialize_payloads(
    tensors: Iterable[tuple[str, np.ndarray]], start_offset: int
) -> list[TensorPayload]:
    payloads: list[TensorPayload] = []
    offset = start_offset
    for name, values in tensors:
        offset = align(offset, PAYLOAD_ALIGNMENT_BYTES)
        payloads.append(TensorPayload(name=name, values=values, offset=offset))
        offset += values.nbytes
    return payloads


def write_atomically(path: Path, metadata: bytes, payloads: list[TensorPayload]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if len(metadata) > FIRST_PAYLOAD_OFFSET:
        raise ValueError(
            f"FlatBuffer metadata is {len(metadata)} bytes and exceeds the "
            f"{FIRST_PAYLOAD_OFFSET}-byte payload boundary"
        )
    if len(metadata) > METADATA_REGION_BYTES:
        raise ValueError("FlatBuffer metadata exceeds LiteRT-LM's one-MiB map")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(metadata)
            for payload in payloads:
                current = output.tell()
                if current > payload.offset:
                    raise ValueError("computed LoRA payload offsets overlap metadata or data")
                output.write(b"\0" * (payload.offset - current))
                output.write(payload.values.tobytes(order="C"))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def inspect_output(path: Path, expected_count: int, rank: int) -> None:
    with path.open("rb") as source:
        metadata_region = source.read(METADATA_REGION_BYTES)
    model = tflite.Model.GetRootAsModel(metadata_region, 0)
    if model.SubgraphsLength() != 1:
        raise ValueError("generated adapter must contain exactly one subgraph")
    subgraph = model.Subgraphs(0)
    if subgraph.TensorsLength() != expected_count:
        raise ValueError("generated adapter tensor count changed during serialization")
    if model.MetadataLength() != 1:
        raise ValueError("generated adapter is missing lora_rank metadata")
    metadata = model.Metadata(0)
    if metadata.Name() != b"lora_rank" or metadata.Buffer() != rank:
        raise ValueError("generated adapter contains incorrect lora_rank metadata")
    file_size = path.stat().st_size
    for index in range(subgraph.TensorsLength()):
        tensor = subgraph.Tensors(index)
        buffer = model.Buffers(tensor.Buffer())
        if buffer.DataLength() != 0:
            raise ValueError("generated adapter contains an inline tensor buffer")
        if buffer.Offset() < FIRST_PAYLOAD_OFFSET or buffer.Size() == 0:
            raise ValueError("generated adapter contains an invalid external buffer")
        if buffer.Offset() + buffer.Size() > file_size:
            raise ValueError("generated adapter buffer exceeds the artifact size")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--safetensors", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dtype", choices=("f32", "f16"), default="f32")
    parser.add_argument(
        "--tensor-naming", choices=("compiled", "legacy"), default="compiled"
    )
    parser.add_argument("--hidden-size", type=int, default=2048)
    parser.add_argument("--kv-size", type=int, default=256)
    parser.add_argument(
        "--scale",
        type=float,
        help="override lora_parameters.scale from the MLX-LM adapter config",
    )
    parser.add_argument(
        "--allow-partial-attention",
        action="store_true",
        help=(
            "explicitly omit adapter targets not exposed by the compiled "
            "LiteRT-LM attention-only LoRA signatures"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = json.loads(args.config.read_text())
    rank = int(config["lora_parameters"]["rank"])
    scale = float(
        args.scale
        if args.scale is not None
        else config["lora_parameters"]["scale"]
    )
    layers = int(config["num_layers"])
    declared_targets = config["lora_parameters"]["keys"]
    if (
        not isinstance(declared_targets, list)
        or not declared_targets
        or not all(isinstance(target, str) and target for target in declared_targets)
        or len(set(declared_targets)) != len(declared_targets)
    ):
        raise ValueError("lora_parameters.keys must contain unique non-empty strings")
    if (
        rank <= 0
        or layers <= 0
        or args.hidden_size <= 0
        or args.kv_size <= 0
        or not np.isfinite(scale)
        or scale <= 0
    ):
        raise ValueError(
            "rank, layers, hidden size, KV size, and LoRA scale must be positive"
        )
    tensors, ignored_targets, ignored_tensor_count = load_attention_tensors(
        args.safetensors,
        layers=layers,
        rank=rank,
        hidden_size=args.hidden_size,
        kv_size=args.kv_size,
        scale=scale,
        dtype=args.dtype,
        naming=args.tensor_naming,
        declared_targets=declared_targets,
        allow_partial_attention=args.allow_partial_attention,
    )
    payloads = materialize_payloads(tensors, FIRST_PAYLOAD_OFFSET)
    metadata = build_metadata(payloads, rank, args.dtype)
    write_atomically(args.output, metadata, payloads)
    inspect_output(args.output, len(payloads), rank)
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "bytes": args.output.stat().st_size,
                "rank": rank,
                "scale": scale,
                "dtype": args.dtype,
                "tensor_naming": args.tensor_naming,
                "tensor_count": len(payloads),
                "source_tensor_count": len(payloads) + ignored_tensor_count,
                "ignored_tensor_count": ignored_tensor_count,
                "ignored_targets": ignored_targets,
                "partial_adapter": bool(ignored_targets),
                "metadata_bytes": len(metadata),
                "external_buffers": True,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
