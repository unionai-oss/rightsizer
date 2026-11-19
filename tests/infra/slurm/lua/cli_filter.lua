-- Pass-through cli_filter.lua seeded by tests/infra/slurm.sh. Loaded by
-- sbatch/salloc/srun on every invocation.
function slurm_cli_setup_defaults(options, early_pass)
  return slurm.SUCCESS
end

function slurm_cli_pre_submit(options, pack_offset)
  return slurm.SUCCESS
end

function slurm_cli_post_submit(offset, job_id, step_id)
  return slurm.SUCCESS
end
