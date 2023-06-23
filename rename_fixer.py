import os, json, uuid, subprocess
from pathlib import Path
from os import scandir

root = Path(".")

def scantree(path):
    """Recursively yield DirEntry objects for given directory."""
    for entry in scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield Path(entry.path)
            yield from scantree(entry.path)  # see below for Python 2.x
        else:
            yield Path(entry.path)

job_dirs = []
for pd in scantree(Path(".")):
    if pd.is_dir() and pd.parent.name == "jobs":
        job_dirs.append(pd)

print(f"Found {len(job_dirs)} total jobs")

#regen_jobs = []
print("*" * 10)
print("The following jobs need to be regenerated. Steps:")
print("1. Commit local changes create by this script and push them")
print("2. Pull changes to branch in Prophecy")
print("3. Open each Job listed below in Prophecy")
print("4. Commit changes in Prophecy")
print("5. Pull branch to your local machine (update)")
print("6. Run script again to verify")
print("*" * 10)
any_regen = False
for job_dir in job_dirs:
    code_dir = Path(job_dir) / "code"

    pr_file = code_dir / "prophecy-job.json"
    with open(pr_file, "r") as f:
        pr_json = json.loads(f.read())
    job_name = pr_json["metainfo"]["name"]
    
    regen_job = False
    db_file = code_dir / "databricks-job.json"
    if not db_file.exists():
        print(f"{job_name}: Databricks workflow file missing, ensure you've opened the job in Prophecy")
        regen_job = True
        continue
    else:
        with open(db_file, "r") as f:
            db_json = json.loads(f.read())
        
        pipeline_ids = {}
        for (pid, process) in pr_json["processes"].items():
            pipeline_ids[process["properties"]["pipelineId"]] = pid
        
        broken_pipelines = []
        for cluster in db_json["request"]["job_clusters"]:
            if "spark_conf" in cluster["new_cluster"]:
                spark_conf = cluster["new_cluster"]["spark_conf"]
                if "prophecy.packages.path" in spark_conf:
                    package_json = json.loads(spark_conf["prophecy.packages.path"])
                    for (pipeline_id, procid) in pipeline_ids.items():
                        if pipeline_id not in package_json:
                            #print(f"Pipeline ID '{pipeline_id}' missing from job '{job_name}' DBX config.")
                            broken_pipelines.append(procid)
                            regen_job = True
                            #regen_jobs.append(job_dir)
                else:
                    #print(f"DBX Config for job '{job_name}' missing 'prophecy.packages.path'.")
                    #regen_jobs.append(job_dir)
                    regen_job = True
            else:
                #print(f"DBX Config for job '{job_name}' missing 'spark_conf'.")
                #regen_jobs.append(job_dir)
                regen_job = True
    
    if regen_job:
        any_regen = True
        os.remove(db_file.absolute())
        subprocess.check_call(["git", "add", db_file.absolute()])
        print(f"Regen required: {job_name}")

if any_regen:
    print("Some pipelines need to be regenerated. Please check the instructions above")
else:
    print("All Jobs have been verified!")