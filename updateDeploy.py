import sys
import os
import json


dbLoc = "code/databricks-job.json"
env = sys.argv[1]
if env == "prod":
  envFolder = "prd"
else:
  envFolder = env
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

                a = json.loads(task["python_wheel_task"]["parameters"][3])
                b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}')
                a.update(b)
                #print(a)
                task["python_wheel_task"]["parameters"][3] = json.dumps(a) #'{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}'
            
            f.close()
            f = open(item.path+"/"+dbLoc,'w')
            json.dump(jsonObject,f)
