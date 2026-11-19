# rightsizer

Right-size AI workloads before they run: choose memory, CPU and GPU requests
from a workload's code, its inputs' metadata, its platform and its history.
It can recommend or enforce, under an explicit safety contract.

- Rust core, Python bindings (`pip install rightsizer`), and the `rightsize` CLI
- Integrations:
  - orchestrators: **Flyte v2** (task, action, run and environment levels),
    **Metaflow**, **Airflow**
  - schedulers and runtimes: **Kubernetes** (CLI and admission webhook),
    **Slurm**, **Ray**
  - GPU launchers: **SkyPilot**, **dstack**
  - managed services: **SageMaker AI**, **AWS Batch**, **Modal**; Vertex AI,
    Azure ML and Databricks planned
- `rightsize audit`: a read-only waste and risk report from any supported
  platform's history, with nothing installed on the platform
- `rightsize suggest`: resource edits proposed as pull requests
- A platform plugin interface for other schedulers, and an Estimator Protocol
  so the model can be hosted anywhere

**Status:** design. Start with the spec in [`docs/spec/`](docs/spec/SPEC.md).
