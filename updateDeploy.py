import sys
import os
import json
import re
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

            #print(item.path+"/"+dbLoc)
            f = open(item.path+"/"+dbLoc)
            #print(f)
            jsonObject = json.load(f)
            #print(jsonObject)
            jsonObject["request"]["name"] = env+"_"+app+"_"+ jsonObject["request"]["name"]
            ##Auto disable jobs that don't have PIPELINE in their name 
            if "PIPELINE" not in jsonObject["request"]["name"].upper():
                jsonObject["request"]["schedule"]["pause_status"] = "PAUSED"
            for task in jsonObject["request"]["tasks"]:

                if 'python_wheel_task' in task:
                    pipelinePath = getPipelinePath(task["task_key"], jsonObject)
                    packageName = ""
                    d = {}
                    try:
                        import sys
                        pipelinePath = getPipelinePath(task["task_key"], jsonObject)

                        fo = open("./src/"+pipelinePath+"/code/setup.py", "r")
                        setupFile = fo.read()
                        packageName = re.findall("'main = (.*)\.",setupFile)[0]

                        sys.path.insert(0,"./src/"+pipelinePath+"/code/"+packageName)
                        fo.close()

                        #print("./src/"+pipelinePath+"/code/"+task["python_wheel_task"]["package_name"])
                        from config.Config import Config
                        c1 = Config()
                        c1.update()
                        ##Address bug with configs dropping out 
                        d = c1.__dict__
                        d.pop("spark")
                        
                        sys.path.pop(0)
                    except Exception as e:
                        print("ERROR WITH: "+"./src/"+pipelinePath+"/code/"+task["python_wheel_task"]["package_name"] \
                            +"----"+jsonObject["request"]["name"] \
                            +"  cannot find package  " + packageName )
                            # task["python_wheel_task"]["package_name"])
                    

                    orConfig = open("./src/"+pipelinePath+"/code/configs/"
                        + "resources/config/" + task["python_wheel_task"]["parameters"][1] + ".json")
                    a = json.load(orConfig)
                    a.update(d)

                    # if task["python_wheel_task"]["parameters"][1] == "BW2":
                    #     print(a)
                    #components -> nodename ->  get id
                    #loop through  jsonObject["components"]
                    #    task["key"]
                    #
                    #Read pipeline folder + id + /code/configs/task["python_wheel_task"]["parameters"][1] .json
                    # Load that as a 
                    

                    # Then load the overwrites 
                    c = json.loads(task["python_wheel_task"]["parameters"][3])
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
                    #print(a)
                    task["python_wheel_task"]["parameters"][3] = json.dumps(a) #'{"targetSchema":"'+envFolder+'_'+app+'","targetEnv":"'+envFolder+'","targetApp":"'+app+'"}'
                    if task["python_wheel_task"]["parameters"][1] == "BW2":
                        print(a)                
            f.close()
            f = open(item.path+"/"+dbLoc,'w')
            json.dump(jsonObject,f)