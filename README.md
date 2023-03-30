MDH
==============================

# Pre-requisite
1. Install Docker
2. Clone this repo where this README.md lives
3. Request `ITS-ASx-TAGC-CD-Contributors` IDMS group

# Setup
1. Login to TAGC repo : `docker login tagc-docker-release.artifactrepo.jnj.com`

2. Pull the docker image : `docker pull tagc-docker-release.artifactrepo.jnj.com/mdh-robot-suite:latest`

# Run suite via docker
Run the docker image ensuring the following mandatory environment options are provided
* DATABRICKS_TOKEN
* DATABRICKS_URL

* -v option to mount the cloned repo. This must be of the form `-v [local-path]:/tmp/robot`

And the following are optional (defaults)
 * DATABRICKS_HOST (https://westeurope.azuredatabricks.net)
 * SYSTEM_UNDER_TEST (panda)
 * TAGS (-i AAAA-123)
 * TABLE_ROW_LIMIT (1000)   -   Sets the maximum amount of rows for tables under test
 * TABLES_UNDER_TEST (afko afpo)    -   Set specific tables to be tested

NB. If no TABLE_ROW_LIMIT or TABLES_UNDER_TEST specified, by default all tables for that system will be tested. If  TABLE_ROW_LIMIT set, all tables within tables size are tested, If TABLES_UNDER_TEST set only specified tables(of any size will be tested)

Example:

Create *envfile* with
```
DATABRICKS_TOKEN=dapi....66f
DATABRICKS_URL=sql/protocolv1/o/4868754987641305/0119-121904-altos379

```

```
docker run \
--env-file ./envfile \
-v /home/brian/gitdev/mdh_pipeline_test/:/tmp/robot \
-it mdh-robot-suite
```

By default, the TAGS environment option is set as above.
This will run some boilerplate tests. Ideally you want to set TAGS to the parts of the test suite
you want to run OR set it to blank to run everything.

E.g.
Run the full suite:

`docker run ..... -e "TAGS="`

Run using matching tags:

`docker run ..... -e "TAGS=-i ELIMS"`

# Operations
As the local git checkout has been mounted as the volume on the docker run, check the *Output* directory
for program output.

If you need to connect into the docker container (for running tests manually) append "bash" at the end
of the run command. E.g.
```
docker run .... -it mdh-robot-suite bash
```

# Useful commands
Not relevant for running the test suite.

`docker build -t mdh-robot-suite  -f Dockerfile .`

`docker tag mdh-robot-suite tagc-docker-release.artifactrepo.jnj.com/mdh-robot-suite`

`docker push tagc-docker-release.artifactrepo.jnj.com/mdh-robot-suite`

`docker tag mdh-robot-suite tagc-docker-release.artifactrepo.jnj.com/mdh-robot-suite:latest`

`docker push tagc-docker-release.artifactrepo.jnj.com/mdh-robot-suite`

# XRAY UPLOADER
## Upload Test to XRAY
The run_xray_uploader.py file is used to automatically upload test scripts to Jira Xray.
This is achieved by firstly running the test followed by running a script to upload the test scripts.
In RobotRunner set the test tags to run
```
self.all_tests = [{"tag": "test1"},{"tag": "test2"}]
```
By default when creating tests dryrun is set to True by default
```
self.write_env_file(test["tag"], True)
```
Ensure executions are disabled
```
if __name__ == "__main__":
    robot_runner = RobotRunner()
    robot_runner.create_tests()
    #robot_runner.attach_executions()
```

Run the following command
```
python run_xray_uploader.py --token dapi60c9ae1a453b2cf598bxxxxxxxxxxx --url sql/protocolv1/o/4924220490975335/1022-090929-unify170 --environment QA
```
And the following arguments are optional
 * -up, --upload, default="Y"
 * -sys, --system, default self.system
 * -rel, --release, default=""

## Upload executions to Xray
The run_xray_uploader.py file is used to automatically upload test executions to Jira Xray.
This is achieved by firstly running the test followed by running a script to upload the test executions.
In RobotRunner set the test tags to run and the JIRA id of the execution, If no id is specified a new execution will be created.
```
self.all_tests = [{"tag": "test1", "id": "TEXC-0001"},{"tag": "test2", "id":""},{"tag": "test3"}]
```

Ensure create_tests() is disabled
```
if __name__ == "__main__":
    robot_runner = RobotRunner()
    #robot_runner.create_tests()
    robot_runner.attach_executions()
```

Run the following command:
NB the --release flag must be set (jira Affects-version) when creating executions to tag the release in the Execution title
```
python run_xray_uploader.py --token dapi60c9ae1a453b2cf598bxxxxxxxxxxx --url sql/protocolv1/o/4924220490975335/1022-090929-unify170 --environment QA
```
And the following arguments are optional
 * -up, --upload, default="Y" (set to N to disable upload)
 * -sys, --system, default self.system
 * -rel, --release, default=""

# Troubleshooting
* If you get a "TLS Handshake failed" error message make sure
    * Your VPN is running
    * The IP address of tagc-docker-release.artifactrepo.jnj.com is resolving consistently between Windows and a Linux VM (if using)
