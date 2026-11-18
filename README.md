# rightsizer

Right-size AI workloads before they run: choose memory, CPU and GPU requests
from a workload's code, its inputs' metadata, its platform and its history.
It can recommend or enforce, under an explicit safety contract.

- Rust core, Python bindings (`pip install rightsizer`), and the `rightsize` CLI
- Integrations: **Flyte v2** (task, action, run and environment levels),
  **Kubernetes** (CLI and admission webhook), **Slurm**, **Ray**
- A platform plugin interface for other schedulers, and an Estimator Protocol
  so the model can be hosted anywhere

**Status:** design. Start with the spec in [`docs/spec/`](docs/spec/SPEC.md).
