-- Pass-through job_submit.lua seeded by tests/infra/slurm.sh. Tests overwrite
-- this file in tests/infra/.state/slurm-etc-extra/; slurmctld re-reads it when
-- its mtime changes.
function slurm_job_submit(job_desc, part_list, submit_uid)
  return slurm.SUCCESS
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
  return slurm.SUCCESS
end

return slurm.SUCCESS
