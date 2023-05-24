import sys
import os
import json
import re

import importlib.util
import sys



#Used to handle the environment substitution
dbLoc = "code/databricks-job.json"
env = sys.argv[1]


def getPipelinePath(pipelineName, jsonObject):

    for item in jsonObject["components"]:
        #print(item["PipelineComponent"]["nodeName"] + "==" + pipelineName)
        try:
            if item["PipelineComponent"]["nodeName"] == pipelineName:
                return item["PipelineComponent"]["id"]
        except:
            pass

if env == "prod":
  envFolder = "prd"
else:
  envFolder = env

app = sys.argv[2]

for item in os.scandir("./src/jobs/"):
    if item.is_dir():
        if os.path.isfile(item.path+"/"+dbLoc):

            #We read the databricks job file
            f = open(item.path+"/"+dbLoc)
            jsonObject = json.load(f)

            #We update the name to reflect the environment and app name
            jsonObject["request"]["name"] = env+"_"+app+"_"+ jsonObject["request"]["name"]
            ##Auto disable jobs that don't have PIPELINE in their name 
            if "PIPELINE" not in jsonObject["request"]["name"].upper():
                jsonObject["request"]["schedule"]["pause_status"] = "PAUSED"

            #For each task in the pipeline we:
            # Read the default configuration from the python package
            # We overlay the chosen config set
            # We overlay any parameters from the job
            # We overlay our custom overrides for HM2 and for Prod/Non-Prod filtering
            for task in jsonObject["request"]["tasks"]:

                if 'python_wheel_task' in task:
                    targetConfig = task["python_wheel_task"]["parameters"][1]
                    pipelinePath = getPipelinePath(task["task_key"], jsonObject)
                    packageName = ""
                    d = {}
                    a = {}
                    try:
                        
                        fo = open("./src/"+pipelinePath+"/code/setup.py", "r")
                        setupFile = fo.read()
                        packageName = re.findall("'main = (.*)\.",setupFile)[0]

                        #sys.path.insert(0,"./src/"+pipelinePath+"/code/"+packageName)
                        #print(sys.path)
                        #print("./src/"+pipelinePath+"/code/"+packageName)
                        fo.close()


                        spec = importlib.util.spec_from_file_location("module.name", "./src/"+pipelinePath+"/code/"+packageName+"/config/Config.py")
                        foo = importlib.util.module_from_spec(spec)
                        sys.modules["module.name"] = foo
                        spec.loader.exec_module(foo)
                        c2 = foo.Config()

                        c2.update()
                        ##Address bug with configs dropping out 
                        #print(c2.__class__)
                        a = c2.__dict__

                        #if task["python_wheel_task"]["parameters"][1] == "BW2":
                        #   print(task["task_key"])
                        #    print(jsonObject["request"]["name"] + str(c2.__dict__))  
                        #    print("./src/"+pipelinePath+"/code/"+packageName)   
                        a.pop("spark")
                        #sys.path.pop(0)
                    except Exception as e:
                        print("ERROR WITH: "+"./src/"+pipelinePath+"/code/"+task["python_wheel_task"]["package_name"] \
                            +"----"+jsonObject["request"]["name"] \
                            +"  cannot find package  " + packageName )
                            # task["python_wheel_task"]["package_name"])
                    

                    orConfig = open("./src/"+pipelinePath+"/code/configs/"+ "resources/config/" + task["python_wheel_task"]["parameters"][1] + ".json")

                    d = json.load(orConfig)

                    a.update(d)

                    c = json.loads(task["python_wheel_task"]["parameters"][3])
                    #print(c)
                    a.update(c)
                    if env == "prod" and task["python_wheel_task"]["parameters"][1].upper() == "HM2":
                        #print(jsonObject)
                        b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","sourceDatabase":"hmd","sourceSystem":"hmd","nonProdFilter":false}')
                        print("Updating platform environment details for HM2 -> HMD")

                    elif env == "pqa" and task["python_wheel_task"]["parameters"][1].upper() == "HM2":
                        #print(jsonObject)
                        b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","sourceDatabase":"pqa_hm2","sourceSystem":"hm2"}')
                        print("Updating platform environment details for HM2 -> HMD")
                    elif env == "prod":
                        b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","nonProdFilter":false}')
                        
                    else:
                        b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}')
                    a.update(b)
                    task["python_wheel_task"]["parameters"][3] = json.dumps(a) #'{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}'
                    #if task["python_wheel_task"]["parameters"][1] == "BW2":
                    #    print(jsonObject["request"]["name"] + str(a))                
            f.close()
            f = open(item.path+"/"+dbLoc,'w')
            json.dump(jsonObject,f)