import sys
import os
import json

#Used to handle the environment substitution
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
            #print(item.path+"/"+dbLoc)
            f = open(item.path+"/"+dbLoc)
            #print(f)
            jsonObject = json.load(f)
            #print(jsonObject)
            jsonObject["request"]["name"] = env+"_"+app+"_"+ jsonObject["request"]["name"]
            for task in jsonObject["request"]["tasks"]:

                if 'python_wheel_task' in task:
               
                    a = json.loads(task["python_wheel_task"]["parameters"][3])
                    #print(a)
                    if env == "prod" and task["python_wheel_task"]["parameters"][1].upper() == "HM2":
                        #print(jsonObject)
                        b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","sourceDatabase":"hmd","sourceSystem":"hmd","nonProdFilter":"false"}')
                        print("Updating platform environment details for HM2 -> HMD")

                    elif env == "pqa" and task["python_wheel_task"]["parameters"][1].upper() == "HM2":
                        #print(jsonObject)
                        b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","sourceDatabase":"pqa_hm2","sourceSystem":"hm2"}')
                        print("Updating platform environment details for HM2 -> HMD")
                    elif env == "prod":
                        b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","nonProdFilter":"false"}')
                        
                    else:
                        b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}')
                    a.update(b)
                    #print(a)
                    task["python_wheel_task"]["parameters"][3] = json.dumps(a) #'{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}'
                
            f.close()
            f = open(item.path+"/"+dbLoc,'w')
            json.dump(jsonObject,f)