import sys
import os
import json



dbLoc = "code/databricks-job.json"
env = sys.argv[1]
app = sys.argv[2]
for item in os.scandir("./src/jobs/"):
    if item.is_dir():
        if os.path.isfile(item.path+"/"+dbLoc):
            print(item.path+"/"+dbLoc)
            f = open(item.path+"/"+dbLoc)
            #print(f)
            jsonObject = json.load(f)
            jsonObject["request"]["name"] = env+"_"+app+"_"+ jsonObject["request"]["name"]
            for task in jsonObject["request"]["tasks"]:
                if 'python_wheel_task' in task:
                #print(task["python_wheel_task"]["parameters"][3])
                    task["python_wheel_task"]["parameters"][3] = '{"targetSchema":"'+env+'_'+app+'","targetEnv":"'+env+'","targetApp":"'+app+'"}'
            f.close()
            f = open(item.path+"/"+dbLoc,'w')
            json.dump(jsonObject,f)
