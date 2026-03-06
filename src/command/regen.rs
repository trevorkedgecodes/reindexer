use std::path::PathBuf;
use std::process::Command;

use crate::penumbra::RegenerationPlan;
use sqlx::PgPool;

#[derive(clap::Parser)]
pub struct RegenAuto {
    /// The URL for the database where we should store the produced events.
    #[clap(long)]
    database_url: String,

    /// The home directory for the penumbra-reindexer.
    ///
    /// Downloaded large files will be stored within this directory.
    ///
    /// Defaults to `~/.local/share/penumbra-reindexer`.
    /// Can be overridden with --archive-file.
    #[clap(long)]
    home: Option<PathBuf>,

    /// Override the location of the sqlite3 database from which event data will be read.
    /// Defaults to `<HOME>/reindexer_archive.bin`.
    #[clap(long)]
    archive_file: Option<PathBuf>,

    /// If set, use a given directory to store the working reindexing state.
    ///
    /// This allows resumption of reindexing, by reusing the directory.
    #[clap(long)]
    working_dir: Option<PathBuf>,

    /// If set, allows the indexing database to have data.
    ///
    /// This will make the indexer add any data that's not there
    /// (e.g. blocks that are missing, etc.). The indexer will not overwrite existing
    /// data, and simply skip indexing anything that would do so.
    #[clap(long)]
    allow_existing_data: bool,

    #[clap(long)]
    /// Specify a network for which events should be regenerated.
    ///
    /// The sqlite3 database must already have events in it from this chain.
    /// If the chain id in the sqlite3 database doesn't match this value,
    /// the program will exit with an error.
    chain_id: Option<String>,

    /// If set, remove the working directory before starting regeneration.
    ///
    /// This ensures a clean state for regeneration but will remove any
    /// existing regeneration progress.
    #[clap(long)]
    clean: bool,
}

impl RegenAuto {
    pub async fn run(self) -> anyhow::Result<()> {
        async fn target_exists(pool: &PgPool, target_height: u64) -> anyhow::Result<bool> {
            Ok(sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM blocks WHERE height = $1)",
            )
            .bind(i64::try_from(target_height)?)
            .fetch_one(pool)
            .await?)
        }

        async fn max_height_le_target(pool: &PgPool, target_height: u64) -> anyhow::Result<u64> {
            let max_height: i64 = sqlx::query_scalar(
                "SELECT COALESCE(MAX(height), 0) FROM blocks WHERE height <= $1",
            )
            .bind(i64::try_from(target_height)?)
            .fetch_one(pool)
            .await?;
            Ok(u64::try_from(max_height)?)
        }

        async fn global_max_height(pool: &PgPool) -> anyhow::Result<u64> {
            let max_height: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(height), 0) FROM blocks")
                .fetch_one(pool)
                .await?;
            Ok(u64::try_from(max_height)?)
        }

        // Determine chain_id - default to penumbra-1 if not specified
        let chain_id = self.chain_id.as_deref().unwrap_or("penumbra-1");
        let home = self
            .home
            .clone()
            .unwrap_or(crate::files::default_reindexer_home()?);
        let archive_file = crate::files::archive_filepath_from_opts(
            self.home.clone(),
            self.archive_file.clone(),
            self.chain_id.clone(),
        )?;
        let archive = crate::storage::Storage::new(Some(&archive_file), Some(chain_id)).await?;
        let final_target = archive
            .last_height()
            .await?
            .ok_or_else(|| anyhow::anyhow!("archive has no blocks"))?;
        let pg = PgPool::connect(&self.database_url).await?;

        // Determine working dir
        let working_dir = self
            .working_dir
            .clone()
            .unwrap_or(crate::files::default_regen_working_dir(&home, chain_id));

        // Handle clean option - remove working directory if it exists
        if self.clean {
            if working_dir.exists() {
                tracing::info!(
                    "Removing existing working directory: {}",
                    working_dir.display()
                );
                std::fs::remove_dir_all(&working_dir)?;
            } else {
                tracing::info!("Working directory does not exist, nothing to clean");
            }
        }

        // Get the regeneration plan for this chain
        let plan = RegenerationPlan::from_known_chain_id(chain_id).ok_or_else(|| {
            anyhow::anyhow!("no regeneration plan known for chain id '{}'", chain_id)
        })?;

        tracing::info!("starting automatic regeneration for chain: {}", chain_id);
        tracing::debug!(
            "found {} regeneration steps, including migrations",
            plan.steps.len()
        );

        // Get current executable path
        let current_exe = std::env::current_exe()?;

        // Extract stop heights from InitThenRunTo steps that have a last_block
        // The RegenerationPlan already handles the proper sequencing of migrate and run steps
        let mut regen_invocations = Vec::new();

        for (_, step) in &plan.steps {
            if let crate::penumbra::RegenerationStep::InitThenRunTo { last_block, .. } = step {
                regen_invocations.push(*last_block);
            }
        }

        tracing::info!(
            "will execute {} regen commands with stop heights: {:?}",
            regen_invocations.len(),
            regen_invocations
        );

        for (i, stop_height) in regen_invocations.iter().enumerate() {
            let target_height = stop_height.unwrap_or(final_target);
            let mut last_max_le_target: Option<u64> = None;
            let mut attempts_without_progress = 0u32;

            for attempt in 1u32.. {
                let mut cmd = Command::new(&current_exe);
                // Shell out to the internal "regen-step" command, so that the "sys::exit" calls in
                // upstream Penumbra deps don't cause the current penumbra-reindexer process to exit.
                cmd.arg("regen-step")
                    .arg("--chain-id")
                    .arg(chain_id)
                    .arg("--home")
                    .arg(&home)
                    .arg("--working-dir")
                    .arg(&working_dir)
                    .arg("--archive-file")
                    .arg(&archive_file)
                    .arg("--database-url")
                    .arg(&self.database_url);

                if self.allow_existing_data {
                    cmd.arg("--allow-existing-data");
                }

                if let Some(height) = stop_height {
                    cmd.arg("--stop-height").arg(height.to_string());
                    tracing::info!(
                        "executing regen command {} of {} (attempt {}, stop-height: {})",
                        i + 1,
                        regen_invocations.len(),
                        attempt,
                        height
                    );
                } else {
                    tracing::info!(
                        "executing final regen command {} of {} (attempt {}, no stop-height)",
                        i + 1,
                        regen_invocations.len(),
                        attempt
                    );
                }
                tracing::debug!("regen command is: {:?}", cmd);
                let status = cmd.status()?;

                if !status.success() {
                    return Err(anyhow::anyhow!(
                        "regen command {} failed on attempt {} with exit code: {:?}",
                        i + 1,
                        attempt,
                        status.code()
                    ));
                }

                let has_target = target_exists(&pg, target_height).await?;
                let max_le_target = max_height_le_target(&pg, target_height).await?;
                let global_max = global_max_height(&pg).await?;

                tracing::info!(
                    "regen command {} attempt {} progress: target {}, has_target={}, max_le_target={}, global_max={}",
                    i + 1,
                    attempt,
                    target_height,
                    has_target,
                    max_le_target,
                    global_max
                );

                if global_max > target_height {
                    tracing::warn!(
                        "database has blocks above this step target (global_max {} > target {}); continuing until target exists",
                        global_max,
                        target_height
                    );
                }

                if has_target {
                    tracing::info!(
                        "regen command {} completed successfully at target height {}",
                        i + 1,
                        target_height
                    );
                    break;
                }

                if let Some(last) = last_max_le_target {
                    if max_le_target <= last {
                        attempts_without_progress += 1;
                    } else {
                        attempts_without_progress = 0;
                    }
                }
                last_max_le_target = Some(max_le_target);

                if attempts_without_progress >= 3 {
                    anyhow::bail!(
                        "regen command {} made no progress for {} attempts; stuck at max height <= target {} of {}, target presence is {}",
                        i + 1,
                        attempts_without_progress + 1,
                        target_height,
                        max_le_target,
                        has_target
                    );
                }
            }
        }

        tracing::info!("all regeneration steps completed successfully");
        Ok(())
    }
}
