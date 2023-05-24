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
                if "pipelines" in item["PipelineComponent"]["id"]:
                    return item["PipelineComponent"]["id"]
                else:
                    return item["PipelineComponent"]["pipelineId"]

        except:
            return None
    return None

if env == "prod":
  envFolder = "prd"
else:
  envFolder = env

app = sys.argv[2]
#print ("HELLO WORLD")
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
                print("Pausing job: " + jsonObject["request"]["name"] + " as it doesn't have PIPELINE in it's name")
            #TODO: For prod switch them to daily automatically, 
            if env == "prod" and ("PIPELINE" in jsonObject["request"]["name"].upper()):
                #jsonObject["request"]["schedule"]["pause_status"] = "UNPAUSED"
                #"* * ? *"
                #"0 0 8 ? * 7 *"
                currentSchedule = jsonObject["request"]["schedule"]["quartz_cron_expression"].split()
                jsonObject["request"]["schedule"]["quartz_cron_expression"] = currentSchedule[0]+" "+currentSchedule[1]+" "+currentSchedule[2]+" * * ? *"
                print("Switching job to daily: " + jsonObject["request"]["name"] + " for Production environment")
            
            if env == "pqa" and ("HM2_PIPELINE" in jsonObject["request"]["name"].upper() or "HMD_PIPELINE" in jsonObject["request"]["name"].upper()):
                #jsonObject["request"]["schedule"]["pause_status"] = "UNPAUSED"
                jsonObject["request"]["schedule"]["quartz_cron_expression"] = "0 0 12 * * ? *"
                jsonObject["request"]["schedule"]["timezone_id"] = "GMT"
                print("Switching job: " + jsonObject["request"]["name"] + " to daily for PQA (HMD/HM2)")
                

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
                    
                    if pipelinePath != None:
                        orConfig = open("./src/"+pipelinePath+"/code/configs/"+ "resources/config/" + task["python_wheel_task"]["parameters"][1] + ".json")

                        d = json.load(orConfig)

                        a.update(d)

                        c = json.loads(task["python_wheel_task"]["parameters"][3])
                        #print(c)
                        a.update(c)
                        if env == "prod" and task["python_wheel_task"]["parameters"][1].upper() == "HM2":
                            #print(jsonObject)
                            b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","sourceDatabase":"hmd","sourceSystem":"hmd","nonProdFilter":false}')
                            print("Updating task: "+jsonObject["request"]["name"]+"/"+task["python_wheel_task"]["parameters"][3] +"platform environment details for HM2 -> HMD - prod")

                        elif env == "pqa" and task["python_wheel_task"]["parameters"][1].upper() == "HM2":
                            #print(jsonObject)
                            b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","sourceDatabase":"pqa_hm2","sourceSystem":"hm2"}')
                            print("Updating task: "+jsonObject["request"]["name"]+"/"+task["python_wheel_task"]["parameters"][3] +"platform environment details for HM2 -> HMD - pqa")
                        elif env == "prod":
                            b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'","nonProdFilter":false}')
                            print("Updating task: "+jsonObject["request"]["name"]+"/"+task["python_wheel_task"]["parameters"][3] +" environment details for prod")
                        else:
                            b = json.loads('{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}')
                            print("Updating task: "+jsonObject["request"]["name"]+"/"+task["python_wheel_task"]["parameters"][3] +" environment details for " + env)
                        a.update(b)
                        task["python_wheel_task"]["parameters"][3] = json.dumps(a) #'{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}'
                        #if task["python_wheel_task"]["parameters"][1] == "BW2":
                        #    print(jsonObject["request"]["name"] + str(a))
                    else:
                        print(jsonObject)                
            f.close()
            f = open(item.path+"/"+dbLoc,'w')
            json.dump(jsonObject,f)