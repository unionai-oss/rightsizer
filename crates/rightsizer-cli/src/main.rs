//! `rsz`: the rightsizer CLI (SPEC §12).
//!
//! Status: the command surface of SPEC §12.1 is declared; `rsz config show`
//! and `rsz config validate` work; every other command exits 1 with
//! "not implemented yet". Exit codes: 0 success (including "kept the
//! prior"), 2 config error, 3 input parse error, 4 a wrapped submitter failed.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};
use rightsizer_engine::Config;

#[derive(Parser, Debug)]
#[command(name = "rsz", version, about = "Right-size AI workloads before they run")]
struct Cli {
    /// Output format.
    #[arg(long, short = 'o', global = true, value_enum, default_value_t = Format::Text)]
    output: Format,
    #[command(subcommand)]
    command: Command,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Format {
    Text,
    Json,
    Yaml,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Decisions for a submission (mode = recommend).
    Recommend {
        /// Platform.
        #[arg(short, long)]
        platform: Option<String>,
        /// File or `-`.
        file: String,
    },
    /// Rewrite a submission (mode = enforce under SPEC §2.3).
    Apply {
        /// File or `-`.
        file: String,
        /// Output file.
        #[arg(long = "out", short = 'f')]
        out: Option<PathBuf>,
        /// Show a diff.
        #[arg(long)]
        diff: bool,
        /// Resubmit on OOM (Ladder).
        #[arg(long)]
        watch: bool,
    },
    /// Distributions, cost table, constraints, holds, advisories.
    Explain {
        /// File or `-`.
        file: Option<String>,
        /// A logged decision.
        #[arg(long)]
        decision: Option<String>,
    },
    /// Savings / would-fail / readiness report (SPEC §12.2).
    Shadow {
        #[arg(long)]
        tenant: Option<String>,
        #[arg(long)]
        key: Option<String>,
        #[arg(long, default_value = "14d")]
        since: String,
    },
    /// Import, show, export or compact history.
    History {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// Read-only waste and risk report (SPEC §12.3).
    Audit {
        platform: String,
        #[arg(long, default_value = "30d")]
        since: String,
    },
    /// Resource edits to code and config (SPEC §12.4).
    Suggest {
        paths: Vec<PathBuf>,
        #[arg(long)]
        write: bool,
        #[arg(long)]
        pr: bool,
    },
    /// MCP server for coding agents (SPEC §12.5).
    Mcp,
    /// Manual outcome record.
    Outcome {
        key: String,
        #[arg(long)]
        status: String,
        #[arg(long)]
        memory_peak: Option<String>,
    },
    /// Breakers: status | trip [--fleet|KEY] | reset [KEY].
    Breaker {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// Fleet control: mode-ceiling [off|observe|recommend|enforce].
    Control {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// Daemon (plugins.md §3).
    Serve {
        #[arg(long)]
        socket: Option<PathBuf>,
        #[arg(long)]
        listen: Option<String>,
    },
    /// Configuration.
    Config {
        #[command(subcommand)]
        action: ConfigCmd,
    },
    /// Plugins: list | check NAME.
    Plugin {
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// Platform subcommands (`rsz k8s …`, `rsz slurm …`, `rsz flyte …`, …).
    #[command(external_subcommand)]
    Platform(Vec<String>),
}

#[derive(Subcommand, Debug)]
enum ConfigCmd {
    /// Print the effective configuration.
    Show,
    /// Validate the configuration (exit 2 on error).
    Validate,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Config { action } => config(action, cli.output),
        other => {
            eprintln!("rsz: {} is not implemented yet", name(&other));
            ExitCode::from(1)
        }
    }
}

fn name(c: &Command) -> String {
    match c {
        Command::Platform(args) => args.first().cloned().unwrap_or_default(),
        other => format!("{other:?}").split([' ', '{', '(']).next().unwrap_or_default().to_lowercase(),
    }
}

fn config(action: ConfigCmd, out: Format) -> ExitCode {
    let loaded = match Config::load() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("rsz: config error: {e}");
            return ExitCode::from(2);
        }
    };
    match action {
        ConfigCmd::Validate => {
            match &loaded.source {
                Some(p) => println!("ok: {}", p.display()),
                None => println!("ok: no config file (defaults)"),
            }
            ExitCode::SUCCESS
        }
        ConfigCmd::Show => {
            let text = match out {
                Format::Text => loaded.config.to_toml(),
                Format::Json => serde_json::to_string_pretty(&loaded.config).unwrap_or_default(),
                Format::Yaml => serde_norway::to_string(&loaded.config).unwrap_or_default(),
            };
            if let Some(p) = &loaded.source {
                eprintln!("# from {}", p.display());
            }
            for o in &loaded.overrides {
                eprintln!("# override {o}");
            }
            print!("{text}");
            ExitCode::SUCCESS
        }
    }
}
