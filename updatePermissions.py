import sys
import os
import json
import requests
from databricks_cli.sdk import DbfsService, JobsService
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider

def _verify_databricks_configs(exit_on_failure=True):
        host = os.environ.get("DATABRICKS_HOST")
        token = os.environ.get("DATABRICKS_TOKEN")

        if host is None or token is None:
            if exit_on_failure:
                print(
                    "[i]DATABRICKS_HOST[/i] & [i]DATABRICKS_TOKEN[/i] environment variables are required to " + 
                    "deploy your Databricks Workflows"
                    )
            else:
                return False
        return True


def set_job_permissions(workspace_url, access_token, job_id, group, permission_level):
    url = f"{workspace_url}/api/2.0/permissions/jobs/{job_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    data = {
        "access_control_list": [
            {"group_name": group, "permission_level": permission_level}
        ]
    }
    data = json.dumps(data)
    response = requests.patch(url, headers=headers, data=data)
    return response.text


def list_jobs(workspace_url, access_token):
    url = f"{workspace_url}/api/2.0/jobs/list/"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    return response.text


def get_workspace_url(env):
    if env == "qa" or env == "QA":
        workspace_url = "https://adb-3309966811984132.12.azuredatabricks.net"
    elif env == "prod" or env == "PROD":
        workspace_url = "https://adb-7108733885606347.7.azuredatabricks.net"
    else:
        workspace_url = "https://adb-4924220490975335.15.azuredatabricks.net"
    return workspace_url


def get_permissions(env):
    if env == "qa" or env == "QA":
        permissions = [
      {
        "name": "ITS-EP-AZR-AJF-DatabricksDataAdministrators",
        "scope": "CAN_MANAGE"
      },
      {
        "name": "ITS-EP-AZR-AJF-DatabricksDataOperators",
        "scope": "CAN_MANAGE"
      },
      {
        "name": "ITS-EP-AZR-AJF-DatabricksDataMonitors",
        "scope": "CAN_VIEW"
      }
    ]

    elif env == "prod" or env == "PROD" :
        permissions = [
      {
        "name": "ITS-EP-AZR-AJL-DatabricksDataAdministrators",
        "scope": "CAN_MANAGE"
      },
      {
        "name": "ITS-EP-AZR-AJL-DatabricksDataOperators",
        "scope": "CAN_MANAGE"
      },
      {
        "name": "ITS-EP-AZR-AJL-DatabricksDataMonitors",
        "scope": "CAN_VIEW"
      }
    ]
    else:
        permissions = [
      {
        "name": "ITS-EP-AZR-AJC-DatabricksDataAdministrators",
        "scope": "CAN_MANAGE"
      },
      {
        "name": "ITS-EP-AZR-AJC-DatabricksDataOperators",
        "scope": "CAN_MANAGE"
      },
      {
        "name": "ITS-EP-AZR-AJC-DatabricksDataMonitors",
        "scope": "CAN_VIEW"
      }
    ]
    return permissions



_verify_databricks_configs()
dbLoc = "code/databricks-job.json"
env = sys.argv[1]
app = sys.argv[2]
api_client = None

config = EnvironmentVariableConfigProvider().get_config()

api_client = _get_api_client(config)
jobs_service = JobsService(api_client)

limit = 25
current_offset = 0
found_job = None
job_request = {'name':'hello'}
try:
    while found_job is None:
        print(
            f"    Querying existing jobs to find current job: Offset: {current_offset}, Pagesize: {limit}"
        )
        response = jobs_service.list_jobs(
            limit=limit, offset=current_offset, version="2.1"
        )
        current_offset += limit

        found_jobs = response["jobs"] if "jobs" in response else []
        for potential_found_job in found_jobs:
            if (
                str(env+"_"+app) in potential_found_job["settings"]["name"]       
            ):

                for permission in get_permissions(env):
                    print(set_job_permissions(
                        get_workspace_url(env), os.environ.get("DATABRICKS_TOKEN"), potential_found_job["job_id"], permission["name"], permission["scope"]
                    ))
                #found_job = potential_found_job
                #break

        if len(found_jobs) <= 0:
            break

    # job_request = job_definition["request"]
    # if "CreateNewJobRequest" in job_request.keys():
    #     job_request = job_definition["request"]["CreateNewJobRequest"]

    # if found_job is None:
    #     print("    Creating a new job: %s" % (job_request["name"]))
    #     jobs_service.create_job(**job_request)
    # else:
    #     print("    Updating an existing job: %s" % (job_request["name"]))
    #     jobs_service.reset_job(
    #         found_job["job_id"], new_settings=job_request, version="2.1"
    #     )
except Exception as e:
    print(e)
    print(
        f"\n[bold red] Create/Update for job: {job_request['name']} failed with exception: {e} [/bold red]"
    )
    # job_update_failures[job_request["name"]] = str(e)


