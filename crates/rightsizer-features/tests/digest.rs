//! Digest stability: edits that cannot change memory use keep the digest;
//! code changes change it.

use rightsizer_features::code::{Language, code_digest_with, normalize};
use rightsizer_features::{code_digest, code_tokens};

const BASE_PY: &str = r#"import flyte
import pandas as pd

env = flyte.TaskEnvironment(name="etl", resources=flyte.Resources(cpu=2, memory="4Gi"))

@env.task
def run(path: str) -> int:
    """Load and aggregate."""
    df = pd.read_parquet(path, columns=["a", "b"])
    return int(df.groupby("a").b.sum().max())
"#;

#[test]
fn python_cosmetic_edits_keep_the_digest() {
    let variants = [
        // Comments.
        BASE_PY.replace("import flyte\n", "import flyte  # the SDK\n# a comment line\n"),
        // Whitespace and blank lines.
        BASE_PY.replace("df = pd.read_parquet(path, columns=[\"a\", \"b\"])", "df   =  pd.read_parquet(path,  columns=[\"a\", \"b\"])  \n\n\n"),
        BASE_PY.replace("\n\n", "\n\n\n\n").replace("    ", "\t"),
        // Resource directives.
        BASE_PY.replace("flyte.Resources(cpu=2, memory=\"4Gi\")", "flyte.Resources(cpu=(4, 8), memory=\"64Gi\", gpu=\"A100:1\")"),
        BASE_PY.replace("@env.task", "@env.task  # resources tuned by rightsizer"),
    ];
    let d = code_digest(BASE_PY);
    for v in &variants {
        assert_eq!(code_digest(v), d, "cosmetic edit changed the digest:\n{v}\n--- normalized ---\n{}", normalize(v, Language::Python));
    }
}

#[test]
fn python_code_edits_change_the_digest() {
    let d = code_digest(BASE_PY);
    for v in [
        BASE_PY.replace("columns=[\"a\", \"b\"]", "columns=[\"a\"]"),
        BASE_PY.replace("read_parquet", "read_csv"),
        BASE_PY.replace(".max()", ".min()"),
        BASE_PY.replace("\"a\"", "'a '"),
        BASE_PY.replace("Load and aggregate.", "Load, then aggregate."),
    ] {
        assert_ne!(code_digest(&v), d, "code edit kept the digest:\n{v}");
    }
}

#[test]
fn ray_and_options_resources_are_masked() {
    let a = "@ray.remote(num_cpus=2, num_gpus=1, memory=4 * 1024**3)\ndef f(x):\n    return x\nf.options(memory=2**30).remote(1)\n";
    let b = "@ray.remote(num_cpus=8, num_gpus=2, memory=64 * 1024**3)\ndef f(x):\n    return x\nf.options(memory=2**34).remote(1)\n";
    assert_eq!(code_digest(a), code_digest(b));
    assert_ne!(code_digest(a), code_digest(&a.replace("remote(1)", "remote(2)")));
}

#[test]
fn sbatch_directive_edits_keep_the_digest() {
    let a = "#!/bin/bash\n#SBATCH --job-name=train\n#SBATCH --mem=16G\n#SBATCH --cpus-per-task=4\n#SBATCH --time=01:00:00\nmodule load cuda\nsrun python train.py --lr 0.1\n";
    let b = "#!/bin/bash\n#SBATCH --job-name=train\n#SBATCH --mem=128G\n#SBATCH --cpus-per-task=32\n#SBATCH --gres=gpu:4\n#SBATCH --time=12:00:00\n\nmodule load cuda   # toolchain\nsrun --mem=128G -c 32 python train.py --lr 0.1\n";
    assert_eq!(code_digest(a), code_digest(b));
    assert_ne!(code_digest(a), code_digest(&a.replace("--lr 0.1", "--lr 0.2")));
    assert_ne!(code_digest(a), code_digest(&a.replace("module load cuda", "module load cuda/12")));
}

#[test]
fn yaml_resource_edits_keep_the_digest() {
    let a = "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: t\nspec:\n  template:\n    spec:\n      containers:\n      - name: main\n        image: img:1\n        resources:\n          requests: {cpu: \"1\", memory: 2Gi}\n        args: [\"--n\", \"10\"]\n";
    let b = a.replace("requests: {cpu: \"1\", memory: 2Gi}", "requests:\n            cpu: \"8\"\n            memory: 32Gi\n          limits:\n            memory: 32Gi");
    assert_eq!(code_digest(a), code_digest(&b));
    assert_ne!(code_digest(a), code_digest(&a.replace("img:1", "img:2")));
    assert_ne!(code_digest(a), code_digest(&a.replace("\"10\"", "\"11\"")));
}

#[test]
fn digest_is_stable_across_calls_and_pinned() {
    assert_eq!(code_digest(BASE_PY), code_digest(BASE_PY));
    // Pinned: a change in normalization is a features-version change.
    assert_eq!(code_digest_with("x = 1  # c\n", Some(Language::Python)), code_digest_with("x = 1", Some(Language::Python)));
    assert_eq!(code_digest_with("x = 1\n", Some(Language::Python)), format!("sha256:{}", sha256_hex(b"x = 1")));
}

fn sha256_hex(b: &[u8]) -> String {
    rightsizer_core::digest::sha256_hex(b)
}

#[test]
fn tokens_are_deterministic_and_ignore_nothing_the_tuner_saw() {
    let t1 = code_tokens(BASE_PY);
    let t2 = code_tokens(BASE_PY);
    assert_eq!(t1, t2);
    for want in ["tok:read_parquet", "tok:columns=", "tok:groupby", "tok:pd.read_parquet"] {
        assert!(t1.iter().any(|t| t == want), "{want}");
    }
}
