import os
import yaml

PROJECT_FOLDER="src"


with open("diff.txt",'r') as diff:
    pipelinesToKeep = ["pipelines/"+line.rstrip() for line in diff]

if len(pipelinesToKeep) == 0:
    print("Diff is empty we will test everything")
    exit(0)

print("Keeping following pipelines: " +str(pipelinesToKeep))

with open(f"{PROJECT_FOLDER}/pbt_project.yml",'r') as yamlfile:
        projectYAML = yaml.safe_load(yamlfile)
                                     
jobToKeep = ""                                
originalJobs = projectYAML["jobs"].copy()
originalPipelines = projectYAML["pipelines"].copy()
for key,job in originalJobs.items():
    if key != jobToKeep:
         projectYAML["jobs"].pop(key)
            
            
            
for key, pipeline in originalPipelines.items():
    if key not in pipelinesToKeep:
         projectYAML["pipelines"].pop(key)


with open(f"{PROJECT_FOLDER}/pbt_project.yml", 'w') as file:
   documents = yaml.dump(projectYAML, file)