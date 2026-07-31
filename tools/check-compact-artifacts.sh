#!/bin/sh
set -eu

check_size() {
  artifact=$1
  maximum=$2
  if [ ! -f "$artifact" ]; then
    echo "missing artifact: $artifact" >&2
    return 1
  fi
  actual=$(wc -c < "$artifact" | tr -d ' ')
  if [ "$actual" -gt "$maximum" ]; then
    echo "$artifact is $actual bytes; budget is $maximum bytes" >&2
    return 1
  fi
  echo "$artifact $actual/$maximum bytes"
}

if [ "$#" -lt 2 ] || [ $(( $# % 2 )) -ne 0 ]; then
  echo "usage: $0 ARTIFACT MAX_BYTES [ARTIFACT MAX_BYTES ...]" >&2
  exit 2
fi

while [ "$#" -gt 0 ]; do
  check_size "$1" "$2"
  shift 2
done
