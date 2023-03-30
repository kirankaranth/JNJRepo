"Driver script executes the robot tests by passing cmd line parameters through this script."

from __future__ import print_function
import argparse
import fnmatch
import os
import subprocess
import sys
import shutil
import traceback
from robot.api import logger as defaultlogger
import driver_data

# Driver Default Variables
INCLUDE_TAG = driver_data.include_tag
TEST_FOLDER = driver_data.test_folder
DEFAULT_BROWSER = driver_data.default_browser
DEFAULT_DATA_SERVER = driver_data.default_data_server


class Driver(object):
    "This class executes the Robot Tests and controls configuration applied to the execution."

    def __init__(
        self,
        readSystemParams=True,
        metadata="_",
        output_dir="Output",
        cleanoutputdir=True,
    ):
        self.curdir = os.path.dirname(os.path.realpath(__file__))
        self.parent_dir = os.path.abspath(os.path.join(self.curdir, os.pardir))
        self.output_dir = os.sep.join([self.parent_dir, output_dir])
        self.include_tags = [INCLUDE_TAG]
        self.exclude_tags = ["ExcludeTest"]
        self.listener = os.path.join(self.parent_dir, "Execution", "ResultsListener.py")
        self.variable = []
        self.variable_file = ""
        self.test_folder = TEST_FOLDER
        self.browser = DEFAULT_BROWSER
        self.test_server = DEFAULT_DATA_SERVER
        self.not_critical = ["NotCritical"]
        self.robot_argument_file = "Robot_Arguments"
        self.arg_separator = """
"""
        self.cleanoutputdir = cleanoutputdir
        self.rerunfailed = False
        self.xunit = False
        self.dryrun = False
        self.tidy = False
        self.screenshot_stop = False
        if readSystemParams:
            self.parse_arguments()
        self.command = ""
        self.metadata = metadata
        self.arg_file_data = ""

    def parse_arguments(self, arguments=""):
        """
        Parses arguments from command line and overwrite the default ones where
        passed by user.
        :param arguments: User arguments passed to override system arguments
        :type arguments: String
        """
        parser = argparse.ArgumentParser("Robot Framework ETL Tests Driver")
        parser.add_argument(
            "-s",
            "--test_server",
            help="Server on which to run the tests e.g. Dev, Test, QA, Prod",
            default="Dev",
            choices=["Dev", "Test", "QA", "Prod"],
        )
        parser.add_argument(
            "-i",
            "--includeTags",
            help="Tags to include in tests",
            default=self.include_tags,
            nargs="*",
        )
        parser.add_argument(
            "-e",
            "--excludeTags",
            help="Tags to exclude in tests",
            default=self.exclude_tags,
            nargs="*",
        )
        parser.add_argument(
            "-o",
            "--output_dir",
            help="Output directory for tests",
            default=self.output_dir,
        )
        parser.add_argument(
            "-l", "--listener", help="Listener for tests", default=self.listener
        )
        parser.add_argument(
            "-vars",
            "--variables",
            nargs="*",
            help="Variable list for the tests in format key1:value1 key2:value2 etc.",
            default=self.variable,
        )
        parser.add_argument(
            "-v",
            "--variableFile",
            help="Variable file for the tests",
            default=self.variable_file,
        )
        parser.add_argument(
            "-b",
            "--browser",
            help="Browser on which to run the tests",
            default=self.browser,
        )
        parser.add_argument(
            "-tf",
            "--test_folder",
            help="Type of test executed",
            default=self.test_folder,
        )
        parser.add_argument(
            "-n",
            "--notCritical",
            help="Tags to mark a test not critical",
            default=self.not_critical,
            nargs="*",
        )
        parser.add_argument(
            "-nc",
            "--notcleanoutputdir",
            action="store_false",
            help="Do not delete output directory before test execution",
            default=self.cleanoutputdir,
        )
        parser.add_argument(
            "-rr",
            "--rerunFailed",
            action="store_true",
            help="Rerun Failed tests once and merge results",
        )
        parser.add_argument(
            "-dr",
            "--dryrun",
            action="store_true",
            help="Executes the tests in dry run to verify no keyword failure",
        )
        parser.add_argument(
            "-xu", "--xunit", action="store_true", help="Create Xunit test results"
        )
        parser.add_argument(
            "-td",
            "--tidy",
            action="store_true",
            help="Tidies up the complete code base in place",
            default=self.tidy,
        )
        parser.add_argument(
            "-st",
            "--screenshot_stop",
            action="store_true",
            help="Stop Screenshot taking if this variable is specified.",
        )
        if arguments:
            args = parser.parse_args(arguments)
        else:
            args = parser.parse_args(sys.argv[1:])
        self.test_server = args.test_server
        self.include_tags = args.includeTags
        self.output_dir = self._get_system_path(args.output_dir)
        self.exclude_tags = args.excludeTags
        self.listener = args.listener
        self.variables = args.variables
        self.variable_file = args.variableFile
        self.browser = args.browser
        self.test_server = args.test_server
        self.test_folder = args.test_folder
        self.not_critical = args.notCritical
        self.cleanoutputdir = args.notcleanoutputdir
        self.rerunfailed = args.rerunFailed
        self.dryrun = args.dryrun
        self.xunit = args.xunit
        self.tidy = args.tidy
        self.screenshot_stop = args.screenshot_stop

    def _add_option_list(self, option, and_separated_list):
        """
        Returns string to add an option with And separated list.

        :param option: Option to add to a command
        :param and_separated_list: List of options to be separated seperated by 'AND'
        :type option: string
        :type and_separated_list: list
        :returns:   Addition of commands to be added to the robot commands
        """
        return (
            " --"
            + option
            + " "
            + "AND".join(and_separated_list)
            + " "
            + self.arg_separator
        )

    def add_option(self, option, value=""):
        """
        Returns string to add an option with single value assigned to it.

        :param option: Option to add to a command
        :param value: Value to be set for the option
        :type option: string
        :type value: string
        :returns:   Addition of command to be added to the robot command
        """
        return " --" + option + " " + value + " " + self.arg_separator

    def add_variable(self, variable, value):
        """
        Returns string to add an variable and value assigned to it.

        :param variable: Variable string to add
        :param value: Value to be set for the variable
        :type variable: string
        :type value: string
        :returns:   Variable value example --variable var1:value
        """
        return " --variable " + variable + ":" + value + " " + self.arg_separator

    @staticmethod
    def _add_quotes_to_path(path):
        """
        Adds double quotes to the start and end of path specified.

        :param path: Folder path to add double quotes to
        :type path: string
        :returns:   Double quoted path e.g. '"path"'
        """
        if not (path.startswith('"') and path.endswith('"')):
            path = '"' + path + '"'
        return path

    @staticmethod
    def _get_system_path(path):
        "Changes the path separator to correct system path."
        if ("/" in path and os.sep != "/") or ("\\" in path and os.sep != "\\"):
            path = path.replace("/", os.sep)
            path = path.replace("\\", os.sep)
        return path

    def clean_output_directory(self, output_directory=""):
        """
        Deletes output directory and then creates it again before the test starts
        for creating clean logs.
        """
        if self.cleanoutputdir:
            if not output_directory:
                output_directory = self.output_dir
            if not os.path.isabs(output_directory):
                dir_path = os.path.dirname(os.path.realpath(__file__))
                dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
                output_directory = os.path.join(dir_path, output_directory)
            if os.path.isdir(output_directory) and self.cleanoutputdir:
                try:
                    print("Deleting existing Output Directory:", output_directory)
                    shutil.rmtree(output_directory)
                except BaseException:
                    print(traceback.format_exc())
            if not os.path.isdir(output_directory):
                print("Creating Output Directory:", output_directory)
                os.makedirs(output_directory)
            self.cleanoutputdir = False

    @staticmethod
    def _delete_filepath(filepath):
        """
        Deletes a file specified by local file path.

        :param filepath: File Path on local filesystem to delete
        :type filepath: string
        """
        try:
            if filepath.startswith('"') and filepath.endswith('"'):
                filepath = filepath[1:-1]
            print("Deleting filepath:", filepath)
            os.remove(filepath)
        except BaseException:
            print("File not found to delete:", filepath)

    def _get_metadata(self):
        """
        Gets the string to be added to names of each output file.

        :return: String to be added to file names in Output
        """
        metadata = ""
        if __name__ != "__main__":
            metadata = "_" + self.metadata
        return metadata

    def _set_variable(self):
        """
        Adds variable option to the command line
        """
        if self.variables:
            for variable in self.variables:
                if type(variable) == list:
                    for singlevariable in variable:
                        self.command += self.add_option("variable", singlevariable)
                else:
                    self.command += self.add_option("variable", variable)

    def _set_variable_file(self):
        """
        Adds variable file option to the command line
        """
        default_file = os.path.join(self.parent_dir, "Execution", "Access_Data.py")
        self.command += self.add_option("variableFile", default_file)
        for variable_file in self.variable_file.split(";"):
            if variable_file:
                self.command += self.add_option("variableFile", variable_file)

    def _set_notcritical_tags(self):
        """
        Adds options for test with tags marked for selecting as not critical tests.
        """
        self.command += self._add_option_list("noncritical", self.not_critical)

    def _set_exclude_tags(self):
        """
        Adds exclusion tags to the robot command.
        """
        self.command += self._add_option_list("exclude", self.exclude_tags)

    def _set_test_server(self):
        """
        Adds the type of test server as a variable to the command
        """
        self.command += self.add_variable("test_server", self.test_server)

    def _set_screenshot_variable(self):
        """
        Sets the screenshot on/off variable from driver script.
        """
        value = True
        if self.screenshot_stop:
            value = False

        self.command += self.add_variable("screenshot_taking", str(value))

    def _set_default_browser(self):
        """
        Sets the browser for running qlik based tests on.
        """
        self.command += self.add_variable("browser", self.browser)

    def _set_include_tags(self, tags=""):
        """
        Adds inclusion tags to the robot command.

        :param tags: Optional extra tags selected for run usually through threads.
        :type tags: String
        """
        if tags:
            itags = self.include_tags[:]
            itags.append(tags)
            tags = itags
        else:
            tags = self.include_tags
        self.command += self._add_option_list("include", tags)

    def _set_listener_file(self):
        """
        Adds listener file option to the robot command.
        """
        if not self.dryrun:
            self.command += self.add_option("listener", self.listener)

    def _set_run_name(self, name):
        """
        Sets the name of the test run for the robot command.

        :param name: Name to be set for the test execution
        :type name: string
        """
        self.command += self.add_option("name", name)

    def _set_run_log(self, logname, tags=""):
        """
        Sets the log name of the test run for the robot command.

        :param logname: Log name to be set for the test execution
        :param tags: Optional tag names to be added to the log name.
        :type logname: string
        :type tags: String
        """
        if tags:
            logname += "_" + tags
        self.command += self.add_option("log", logname)

    def _set_run_report(self, reportname, tags=""):
        """
        Sets the report name of the test run for the robot command.

        :param reportname: Report name to be set for the test execution
        :param tags: Optional tag names to be added to the log name.
        :type reportname: string
        :type tags: String
        """
        if tags:
            reportname += "_" + tags
        self.command += self.add_option("report", reportname)

    def set_tag(self, tag):
        """
        Sets a tag for all tests running for the test.

        :param tag: Tag to be set for all tests
        :type tag: string
        """
        self.command += self.add_option("settag", tag)

    def _set_options_in_arguments_file(self, tags=""):
        """
        Sets robot options into an argument file.

        :param tags: Optional tag names to be added to the log name.
        :type tags: String
        """
        file_path = self.robot_argument_file + self._get_metadata() + tags
        file_path = os.sep.join([self.parent_dir, file_path])
        self.arg_file_data = self.command
        with open(file_path, "w") as fileobject:
            fileobject.write(self.command)
        file_path = self._add_quotes_to_path(file_path)
        self.command = " --argumentfile " + file_path + " "

    def _add_xunit_option(self, filename):
        """
        Adds xunit results option.

        :param filename: Name of the file which will be created
        :type filename: string
        """
        if self.xunit:
            output_xml = os.sep.join([self.output_dir, filename])
            self.command += self.add_option("xunit", output_xml)

    def _set_execution_folder(self):
        """
        Sets the final execution folder for the driver script.
        """
        self.command += " " + self._add_quotes_to_path(self.test_folder) + " "

    def _set_rerun_xml(self, xml_run):
        """
        Gets quoted file path for xml rerun path

        :param xml_run: Xml result file generated from previous run
        :type xml_run: string
        :return: Complete path for xml rerun
        """
        path = os.sep.join([self.output_dir, xml_run])
        self.command += self.add_option("rerunfailed", self._add_quotes_to_path(path))

    def _set_output_folder(self, rerunxml="", tag=""):
        """
        Sets Output folder and logging xml file for the test run.

        :param rerunxml: Rerunning failed tests xml
        :param tag: Thrad tag that needs to be included for test run.
        :type rerunxml: string
        :type tag: String
        """
        self.command += self.add_option("outputdir", self.output_dir)
        if not rerunxml:
            filename = "Output.xml"
            if tag:
                filename = "Output_" + tag + ".xml"
            self.command += self.add_option("output", filename)
        else:
            self.command += self.add_option("output", rerunxml)

    def _dryrun_execution(self):
        "Adds option to do dryrun for the tests based on user value from command line."
        if self.dryrun:
            self.command += self.add_option("dryrun")
            self.dryrun = False

    @staticmethod
    def _set_environment_variable(variable, value=""):
        """
        Sets environment variable to a given value. If no value is
        passed then sets it to blank
        :param variable: Variable to set as environment variable.
        :param value: Value of the variable to be set.
        """
        print("Setting Environment variable: ")
        print(variable, "=", value)
        os.environ[variable] = value

    @staticmethod
    def _get_environment_variable(variable):
        """
        Reads and fetches an environment variable if it is present.
        :param variable: Environment variable to read from system
        :return: Value of the Environment variable read from system
        """
        envvalue = ""
        if variable in os.environ:
            envvalue = os.environ[variable]
        return envvalue

    def _delete_options_arguments_file(self, tags=""):
        """
        Cleans up by deleting an existing robot arguments file

        :param tags: Tags to uniquely identify log report file.
        :type tags: String
        """
        filepath = self.robot_argument_file + self._get_metadata() + tags
        filepath = os.sep.join([self.parent_dir, filepath])
        print("Deleting arguments file: " + filepath)
        self.arg_file_data = ""
        os.remove(filepath)

    @staticmethod
    def get_python_executable():
        """
        Returns string equivalent to python executable location.
        PYTHON_EXECUTABLE environment variable should be set by the
        virtual environment executing the driver script.

        :return: Python executable command.
        """
        python_exe = sys.executable
        if os.sep in python_exe:
            python_exe = Driver._add_quotes_to_path(python_exe)
        return python_exe

    def _set_execution_command(self):
        """
        Sets the execution command by adding 'robot ' at the command's start
        """
        python_command = self.get_python_executable()
        self.command = python_command + " -m robot " + self.command

    def _set_merge_command(self, executed_xml, rerun_xml):
        """
        Sets self.command to merge after re-running the failed test.

        :param executed_xml: Name of the xml which is previously executed
        :param rerun_xml: Name of the result xml which is created on rerun
        :type executed_xml: string
        :type rerun_xml: string
        :return: Re-run xml filepath to delete
        """
        self.command = self.add_option(
            "outputdir", self._add_quotes_to_path(self.output_dir)
        )
        self.command += self.add_option("output", executed_xml)
        self.add_option("nostatusrc", "")
        executed_xml = self._add_quotes_to_path(
            os.sep.join([self.output_dir, executed_xml])
        )
        rerun_xml = self._add_quotes_to_path(os.sep.join([self.output_dir, rerun_xml]))
        python_command = self.get_python_executable()
        self.command = (
            python_command
            + " -m robot.rebot "
            + self.command
            + " --merge "
            + executed_xml
            + " "
            + rerun_xml
        )
        return rerun_xml

    @staticmethod
    def _get_arg_data_excluding_password(filedata):
        """
        Fetches the content of argument data without logging the password.

        :param filedata: Data read from arguments file.
        :type filedata: String
        :return: file data without the password line
        """
        parsed_data = ""
        splitter = "\r"
        if filedata.count("\n"):
            splitter = "\n"
        for data in filedata.split(splitter):
            if "ssword:" not in data:
                parsed_data += data.strip() + "\n"
        return parsed_data

    def execute_command(self):
        """
        Executes the command on the running machine.
        """
        defaultlogger.console("\nExecuting: " + self.command)
        return_code = 1
        if self.arg_file_data:
            arg_data = self._get_arg_data_excluding_password(self.arg_file_data)
            defaultlogger.console("\nArgument File Data:\n" + arg_data)
        try:
            return_code = subprocess.call(self.command, shell=True)
        except BaseException:
            print(traceback.format_exc())
        return return_code

    def _get_build_command(self, tags):
        "Combines the steps to build the build command"
        self._set_test_server()
        self._set_include_tags(tags)
        self._set_exclude_tags()
        self._set_default_browser()
        self._set_notcritical_tags()
        self._set_variable()
        self._set_variable_file()
        self._set_listener_file()
        self._dryrun_execution()
        self._set_screenshot_variable()

    def rerun_failed_tests(self, tags):
        """
        Reruns the failed tests from previous execution.

        :param tags: Helps identify output log file uniquely.
        :type tags: String
        """
        self.command = ""
        executed_xml = "Output.xml"
        if tags:
            executed_xml = "Output_" + tags + ".xml"
        rerun_xml = "rerun_" + tags + "_" + executed_xml
        self._set_rerun_xml(executed_xml)
        self._get_build_command(tags)
        self._set_run_log("rerun_" + "_LOG" + self._get_metadata(), tags)
        self._set_run_report("rerun_Report" + self._get_metadata(), tags)
        self._set_output_folder(rerun_xml)
        self._set_options_in_arguments_file(tags + "_rerun")
        self._set_execution_folder()
        self._set_execution_command()
        self.execute_command()
        self._delete_options_arguments_file(tags + "_rerun")
        # Merge rerun tests
        rerun_xml = self._set_merge_command(executed_xml, rerun_xml)
        self.execute_command()
        self._delete_filepath(rerun_xml)

    def execute_tests(self, tags=""):
        """
        Performs robot test executions. Different execution command for each of
        the country read from the functional matrix excel.
        """
        os.chdir(self.parent_dir)
        self.clean_output_directory()
        defaultlogger.console("\nExecuting tests from folder: %s" % (self.test_folder))

        self._get_build_command(tags)
        self._set_run_log("LOG", tags)
        self._set_run_report("Report", tags)
        self._set_output_folder(tag=tags)
        self._set_options_in_arguments_file(tags)
        self._set_execution_folder()
        self._set_execution_command()
        return_code = self.execute_command()
        self._delete_options_arguments_file(tags)
        if return_code != 0 and self.rerunfailed and not self.dryrun:
            self.rerun_failed_tests(tags)
        self.command = ""

    def set_output_xml(self):
        """
        Sets the overall output xml file for the executed tests
        """
        outputfile = "OUTPUTXML_" + self.test_folder + ".xml"
        output_xml = os.sep.join([self.output_dir, outputfile])
        self.command += self.add_option("output", output_xml)

    def set_output_html(self):
        """
        Sets the overall output html file for the executed tests
        """
        outputfile = "HTML_" + self.test_folder + ".html"
        output_html = os.sep.join([self.output_dir, outputfile])
        self.command += self.add_option("report", output_html)

    def set_output_log(self):
        """
        Sets the overall output log file for the executed tests
        """
        outputfile = "LOG_" + self.test_folder + ".html"
        output_log = os.sep.join([self.output_dir, outputfile])
        self.command += self.add_option("log", output_log)

    def set_combined_output_location(self):
        """
        Sets the merge command on the output
        """
        self.command += self._add_quotes_to_path(
            os.sep.join([self.output_dir, "*.xml"])
        )

    def set_result_command(self):
        """
        Sets the overall output generating rebot command for the executed tests
        """
        python_command = self.get_python_executable()
        self.command = python_command + " -m robot.rebot " + self.command

    def combine_reports(self):
        """
        Performs the task of overall combining of test reports for each of the countries
        """
        _, _, files = os.walk(os.path.abspath(self.output_dir)).next()
        if not fnmatch.filter(files, "*.xml"):
            print(
                "No output xml files found under output folder: %s" % (self.output_dir)
            )
            return

        self._set_run_name(self.test_folder + "_Tests")
        self._set_exclude_tags()
        self._set_notcritical_tags()
        self._add_xunit_option("xunit_combined" + self._get_metadata())
        self.set_output_xml()
        self.set_output_html()
        self.set_output_log()
        self._set_options_in_arguments_file()
        self.set_combined_output_location()
        self.set_result_command()
        self.execute_command()
        self._delete_options_arguments_file()
        self.command = ""

    def tidy_up_code(self):
        """
        Tidies up the codebase in place. This function should be called
        before making new code update.
        """
        os.chdir(self.parent_dir)
        python_command = self.get_python_executable()
        self.command = (
            python_command + ' -m robot.tidy -r -f robot "' + self.test_folder + '"'
        )
        self.execute_command()

    def robot_tests(self):
        """
        High level execution and combining report functionality
        """
        if self.tidy:
            self.tidy_up_code()
        else:
            self.execute_tests()


if __name__ == "__main__":
    RDRIVER = Driver()
    RDRIVER.robot_tests()
