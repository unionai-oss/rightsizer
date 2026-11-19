//! Running Slurm commands (`sbatch`, `sacct`, `squeue`); a trait so tests can
//! substitute fakes (or fake binaries on `PATH`).

use std::path::Path;

use async_trait::async_trait;

/// A finished command.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CmdOutput {
    /// Exit status was zero.
    pub success: bool,
    /// Exit code (`None` if killed by a signal).
    pub code: Option<i32>,
    /// stdout.
    pub stdout: String,
    /// stderr.
    pub stderr: String,
}

/// Runs a program.
#[async_trait]
pub trait CommandRunner: Send + Sync {
    /// Run `program args…` with extra environment variables.
    async fn run_env(&self, program: &Path, args: &[String], env: &[(String, String)]) -> std::io::Result<CmdOutput>;
    /// Run without extra environment.
    async fn run(&self, program: &Path, args: &[String]) -> std::io::Result<CmdOutput> {
        self.run_env(program, args, &[]).await
    }
}

/// `tokio::process`.
#[derive(Clone, Debug, Default)]
pub struct TokioRunner {
    /// Working directory.
    pub cwd: Option<std::path::PathBuf>,
}

#[async_trait]
impl CommandRunner for TokioRunner {
    async fn run_env(&self, program: &Path, args: &[String], env: &[(String, String)]) -> std::io::Result<CmdOutput> {
        let mut c = tokio::process::Command::new(program);
        c.args(args).stdin(std::process::Stdio::null());
        for (k, v) in env {
            c.env(k, v);
        }
        if let Some(d) = &self.cwd {
            c.current_dir(d);
        }
        let o = c.output().await?;
        Ok(CmdOutput {
            success: o.status.success(),
            code: o.status.code(),
            stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
        })
    }
}
