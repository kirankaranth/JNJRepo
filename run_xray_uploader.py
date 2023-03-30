import os
import pathlib
import getpass
import argparse
import sys


class RobotRunner:
    def __init__(self):
        self.all_tests = [{"tag": "NC_PROD_LINE_ITEM", "id": "AGON-7228"}]
        self.environment = "QA"
        self.system = "l1"

        self.parser = self.create_script_args()
        self.args = self.parse_arguments()
        self.token = self.args.token
        self.url = self.args.url
        self.environment = self.args.environment

        self.mdh_tests_path = pathlib.Path(__file__).parent.absolute()
        self.user = input("Username: ")
        self.user_password = getpass.getpass()

    def create_script_args(self):
        """
        Creates arguments for the script
        :return: parser with arguments
        """
        parser = argparse.ArgumentParser("Parser for XRay tests/results uploader.")
        parser.add_argument("-t", "--token", help="Databricks token.", default="")
        parser.add_argument(
            "-u",
            "--url",
            help="Databricks URL.",
            default="sql/protocolv1/o/4924220490975335/1022-090929-unify170",
        )
        parser.add_argument("-up", "--upload", help="Upload to JIRA?", default="Y")
        parser.add_argument(
            "-env", "--environment", help="Test environment", default=self.environment
        )
        parser.add_argument(
            "-sys", "--system", help="System under test", default=self.system
        )
        parser.add_argument("-rel", "--release", help="Release name", default="")
        return parser

    def parse_arguments(self, script_args=None):
        """
        Parses arguments specified for the robot runner script
        :param script_args: List of script arguments.
        :return: values read from parser
        """
        if script_args is None:
            script_args = sys.argv[1:]
        args = self.parser.parse_args(script_args)
        return args

    def write_env_file(self, tag, dry=False):
        """
        Creates and writes to envfile
        :param tag: Test tag
        :param dry: Is dry run?
        """
        f = open("envfile", "w")
        f.write(f"DATABRICKS_TOKEN={self.token}\n")
        f.write(f"DATABRICKS_URL={self.url}\n")
        if dry:
            f.write(f"TAGS=-i {tag} --dryrun\n")
        else:
            f.write(f"TAGS=-i {tag}\n")
        f.write(f"SYSTEM_UNDER_TEST={self.system}\n")
        f.close()

    def create_tests(self):
        """
        Runs test with dryrun and creates JIRA tasks for them
        """
        for test in self.all_tests:
            self.write_env_file(test["tag"], True)
            self.run_robot_test()
            self.upload_test_scripts()

    def attach_executions(self):
        """
        Runs all tests specified and attaches evidence to execution task (new or provided)
        """
        for test in self.all_tests:
            self.write_env_file(test["tag"])
            self.run_robot_test()

            executions = f"""
                    python XRayUploader/xray_uploader.py \
                    -r {self.mdh_tests_path}/Output/Output.xml \
                    -jprj AGON \
                    -u {self.user} \
                    -p {self.user_password} \
                    -jurl https://jira.jnj.com \
                    -a Add_Result \
                    -xtenv {self.environment}"""

            try:
                if test["id"]:
                    executions += f" -xex {test['id']}"
                else:
                    print(
                        f"[INFO] 'id' attribute not specified for tag {test['tag']} new execution will be created"
                    )
            except KeyError as ke:
                print(
                    f"[INFO] No 'id' attribute provided for tag {test['tag']} new execution will be created"
                )

            if self.args.release:
                executions += f" -rel {self.args.release}"
            if self.args.upload.upper() == "N":
                executions += " --no_jira_upload"

            os.system(executions)

    def run_robot_test(self):
        """
        Runs the Robot test suite.
        Creates the output.xml file used for upload and test reports if dryrun is disabled.
        """
        os.system(
            f"""
                docker run --env-file envfile \
                -v {self.mdh_tests_path}/:/tmp/robot \
                -it tagc-docker-release.artifactrepo.jnj.com/mdh-robot-suite"""
        )

    def upload_test_scripts(self):
        """
        Uploads the test Scripts to XRAY
        :return:
        """
        os.system(
            f"""
                python XRayUploader/xray_uploader.py \
                -r {self.mdh_tests_path}/Output/Output.xml \
                -jprj AGON \
                -u {self.user} \
                -p {self.user_password} \
                -jurl https://jira.jnj.com \
                -a Create_Test"""
        )


if __name__ == "__main__":
    robot_runner = RobotRunner()
    # robot_runner.create_tests()
    robot_runner.attach_executions()
