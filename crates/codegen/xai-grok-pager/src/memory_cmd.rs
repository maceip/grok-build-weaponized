use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;
use serde_json::json;
use xai_grok_shell::session::memory::embedding::LocalEmbeddingProvider;
use xai_grok_shell::session::memory::{
    MemoryBackendImpl, MemoryIndex, init_sqlite_vec, storage::MemoryStorage,
};
use xai_grok_tools::types::memory_backend::MemoryBackend;

#[derive(Debug, clap::Args, Clone)]
pub struct MemoryArgs {
    #[command(subcommand)]
    pub command: MemoryCommand,
}

#[derive(Debug, Subcommand, Clone)]
pub enum MemoryCommand {
    /// Show the workspace memory index and local embedding state
    Status {
        /// Emit machine-readable JSON
        #[arg(long)]
        json: bool,
    },
    /// Rebuild the workspace index and materialize local embeddings
    Reindex {
        /// Build only the FTS5 index and skip vector embeddings
        #[arg(long)]
        fts_only: bool,
        /// Maximum time to wait for the local embedding model to load
        #[arg(long, default_value_t = 180)]
        wait_seconds: u64,
        /// Emit machine-readable JSON
        #[arg(long)]
        json: bool,
    },
    /// Search the current workspace memory
    Search {
        /// Query text
        query: String,
        /// Maximum result count
        #[arg(long, default_value_t = 8)]
        limit: usize,
        /// Minimum merged relevance score
        #[arg(long, default_value_t = 0.0)]
        min_score: f64,
        /// Wait for local embeddings and backfill before searching
        #[arg(long)]
        wait_for_embeddings: bool,
        /// Maximum local model load wait
        #[arg(long, default_value_t = 180)]
        wait_seconds: u64,
        /// Emit machine-readable JSON
        #[arg(long)]
        json: bool,
    },
    /// Clear memory files (workspace by default)
    Clear {
        /// Clear workspace-scoped memory (MEMORY.md, sessions/, index.sqlite)
        #[arg(long, group = "scope")]
        workspace: bool,
        /// Clear global MEMORY.md
        #[arg(long, group = "scope")]
        global: bool,
        /// Clear both workspace and global memory
        #[arg(long, group = "scope")]
        all: bool,
        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

struct ClearTarget {
    label: &'static str,
    path: PathBuf,
    clear: fn(&MemoryStorage) -> std::io::Result<bool>,
}

fn workspace_target(storage: &MemoryStorage) -> ClearTarget {
    ClearTarget {
        label: "workspace memory",
        path: storage.workspace_dir().to_path_buf(),
        clear: |s| s.clear_workspace(),
    }
}

fn global_target(storage: &MemoryStorage) -> ClearTarget {
    ClearTarget {
        label: "global MEMORY.md",
        path: storage.global_memory_file(),
        clear: |s| s.clear_global(),
    }
}

pub async fn run(args: MemoryArgs) -> Result<()> {
    match args.command {
        MemoryCommand::Status { json } => run_status(json),
        MemoryCommand::Reindex {
            fts_only,
            wait_seconds,
            json,
        } => run_reindex(fts_only, wait_seconds, json).await,
        MemoryCommand::Search {
            query,
            limit,
            min_score,
            wait_for_embeddings,
            wait_seconds,
            json,
        } => {
            run_search(
                &query,
                limit,
                min_score,
                wait_for_embeddings,
                wait_seconds,
                json,
            )
            .await
        }
        MemoryCommand::Clear {
            global, all, yes, ..
        } => {
            let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());
            let storage = MemoryStorage::new(&cwd, None);

            let targets = if all {
                vec![workspace_target(&storage), global_target(&storage)]
            } else if global {
                vec![global_target(&storage)]
            } else {
                vec![workspace_target(&storage)]
            };

            run_clear(&storage, &targets, yes)
        }
    }
}

fn workspace_memory() -> Result<(MemoryStorage, xai_grok_shell::config::MemoryConfig)> {
    let cwd = std::env::current_dir()?;
    let raw = xai_grok_shell::config::load_effective_config_disk_only()?;
    let config = xai_grok_shell::config::MemoryConfig::resolve(true, false, &raw, None);
    let storage = if config.flat_memory_root {
        let root = config
            .root_dir_override
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("flat memory root is enabled without a root path"))?;
        MemoryStorage::new_flat(&cwd, root)
    } else {
        MemoryStorage::new(&cwd, config.root_dir_override.as_deref())
    };
    Ok((storage, config))
}

fn open_index(
    storage: &MemoryStorage,
    config: &xai_grok_shell::config::MemoryConfig,
) -> Result<MemoryIndex> {
    init_sqlite_vec();
    let db_path = storage.workspace_dir().join("index.sqlite");
    Ok(MemoryIndex::open_or_create(
        &db_path,
        storage.clone(),
        config.index.clone(),
        config.embedding.dimensions,
    )?)
}

fn run_status(as_json: bool) -> Result<()> {
    let (storage, config) = workspace_memory()?;
    let db_path = storage.workspace_dir().join("index.sqlite");
    let files = storage.list_memory_files()?;
    let (chunks, vector_index, pending_embeddings) = if db_path.exists() {
        let index = open_index(&storage, &config)?;
        (
            storage.total_chunk_count(),
            index.vec_available(),
            index.chunks_without_embeddings()?.len(),
        )
    } else {
        (0, false, 0)
    };
    // Status is observational: do not materialize a 300M model merely to
    // inspect the index. Reindex/search report live load failures when they
    // explicitly request the provider.
    let embedding_status = if config.embedding.model.is_some()
        && matches!(
            config
                .embedding
                .provider
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "local" | "auto"
        ) {
        "configured"
    } else {
        "disabled"
    };
    let value = json!({
        "workspace": storage.workspace_path(),
        "memory_root": storage.workspace_dir(),
        "index": db_path,
        "files": files,
        "chunks": chunks,
        "vector_index": vector_index,
        "pending_embeddings": pending_embeddings,
        "embedding_provider": config.embedding.provider,
        "embedding_model": config.embedding.model,
        "embedding_dimensions": config.embedding.dimensions,
        "embedding_status": embedding_status,
    });
    if as_json {
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        println!("Workspace: {}", storage.workspace_path().display());
        println!("Memory root: {}", storage.workspace_dir().display());
        println!("Files: {}", value["files"].as_array().map_or(0, Vec::len));
        println!("Indexed chunks: {chunks}");
        println!(
            "Vector index: {}",
            if vector_index { "ready" } else { "unavailable" }
        );
        println!("Pending embeddings: {pending_embeddings}");
        println!(
            "Embedding model: {} ({embedding_status})",
            config.embedding.model.as_deref().unwrap_or("off")
        );
    }
    Ok(())
}

async fn run_reindex(fts_only: bool, wait_seconds: u64, as_json: bool) -> Result<()> {
    let (storage, config) = workspace_memory()?;
    let files = storage.list_memory_files()?;
    let mut index = open_index(&storage, &config)?;
    let existing = files
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<std::collections::HashSet<_>>();
    let mut removed_orphans = 0usize;
    for path in index.all_indexed_paths()? {
        if !existing.contains(&path) {
            removed_orphans += index.delete_path(std::path::Path::new(&path))?;
        }
    }
    let mut added = 0usize;
    let mut updated = 0usize;
    let mut removed = removed_orphans;
    for path in &files {
        let stats = index.reindex_file(path, storage.classify_source(path))?;
        added += stats.added;
        updated += stats.updated;
        removed += stats.removed;
    }
    let pending_before = index.chunks_without_embeddings()?.len();
    let embedded = if fts_only || pending_before == 0 {
        0
    } else {
        let provider = LocalEmbeddingProvider::from_config(&config.embedding).ok_or_else(|| {
            anyhow::anyhow!(
                "local embedding provider is not configured; use --fts-only or set [memory.embedding] provider=local"
            )
        })?;
        provider
            .wait_ready(std::time::Duration::from_secs(wait_seconds))
            .await
            .map_err(|error| anyhow::anyhow!("local embedding model failed: {error}"))?;
        xai_grok_shell::session::memory::embed_missing_chunks(&index, &provider).await
    };
    let pending_after = index.chunks_without_embeddings()?.len();
    let value = json!({
        "files": files.len(),
        "chunks": storage.total_chunk_count(),
        "added": added,
        "updated": updated,
        "removed": removed,
        "embedded": embedded,
        "pending_embeddings": pending_after,
        "vector_index": index.vec_available(),
    });
    if as_json {
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        println!(
            "Indexed {} files ({} added, {} updated, {} removed).",
            files.len(),
            added,
            updated,
            removed
        );
        println!("Embedded {embedded} chunks; {pending_after} remain.");
    }
    if !fts_only && pending_after > 0 {
        anyhow::bail!(
            "embedding backfill incomplete: {pending_after} of {pending_before} pending chunks remain"
        );
    }
    Ok(())
}

async fn run_search(
    query: &str,
    limit: usize,
    min_score: f64,
    wait_for_embeddings: bool,
    wait_seconds: u64,
    as_json: bool,
) -> Result<()> {
    if query.trim().is_empty() {
        anyhow::bail!("memory search query cannot be empty");
    }
    let (storage, config) = workspace_memory()?;
    let index = open_index(&storage, &config)?;
    let provider_guard = LocalEmbeddingProvider::from_config(&config.embedding);
    if wait_for_embeddings && let Some(provider) = provider_guard.as_ref() {
        provider
            .wait_ready(std::time::Duration::from_secs(wait_seconds))
            .await
            .map_err(|error| anyhow::anyhow!("local embedding model failed: {error}"))?;
        xai_grok_shell::session::memory::embed_missing_chunks(&index, provider).await;
    }
    drop(index);
    let backend = MemoryBackendImpl::new(
        storage.workspace_dir().join("index.sqlite"),
        storage.clone(),
    )
    .with_session_id("memory-cli".to_string())
    .with_embedding(config.embedding.clone(), String::new(), None)
    .with_search_config(config.search.clone());
    let results = backend
        .search(query, limit.clamp(1, 100), min_score)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    if as_json {
        let value = results
            .iter()
            .map(|result| {
                json!({
                    "chunk_id": result.chunk_id,
                    "path": result.path,
                    "start_line": result.start_line,
                    "end_line": result.end_line,
                    "score": result.score,
                    "source": result.source,
                    "snippet": result.snippet,
                })
            })
            .collect::<Vec<_>>();
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        for result in results {
            println!(
                "{:.4} {}:{}-{} [{}]",
                result.score,
                result.path,
                result.start_line + 1,
                result.end_line,
                result.source
            );
            println!("{}", result.snippet.trim());
            println!();
        }
    }
    Ok(())
}

fn run_clear(storage: &MemoryStorage, targets: &[ClearTarget], skip_confirm: bool) -> Result<()> {
    let existing: Vec<_> = targets.iter().filter(|t| t.path.exists()).collect();

    if existing.is_empty() {
        println!("Nothing to clear \u{2014} no memory files found.");
        return Ok(());
    }

    println!("The following will be deleted:");
    for t in &existing {
        println!("  {}: {}", t.label, t.path.display());
    }

    if !skip_confirm {
        print!("\nAre you sure? [y/N] ");
        std::io::stdout().flush()?;

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    let mut cleared = false;
    let mut errors: Vec<String> = Vec::new();
    for t in targets {
        match (t.clear)(storage) {
            Ok(true) => {
                cleared = true;
                println!("  Cleared: {}", t.label);
            }
            Ok(false) => {} // nothing to clear for this scope
            Err(e) => {
                errors.push(format!("{}: {e}", t.label));
            }
        }
    }

    if cleared && errors.is_empty() {
        println!("Memory cleared.");
    } else if cleared {
        println!("Memory partially cleared. Errors:");
        for e in &errors {
            eprintln!("  {e}");
        }
    } else if !errors.is_empty() {
        eprintln!("Failed to clear memory:");
        for e in &errors {
            eprintln!("  {e}");
        }
        return Err(anyhow::anyhow!("clear failed"));
    }

    Ok(())
}
