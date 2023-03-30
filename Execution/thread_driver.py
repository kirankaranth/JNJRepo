"Executes tests in a thread. Based on tags to parallelize."

from __future__ import print_function
import argparse
import sys
import threading
import yaml
import driver

THREADING_TAGS = ["helloworld", "helloworldfail"]
DEFAULT_THREADS = 5


class ThreadDriver(object):
    """
    Runs the driver file through threads controlled remotely.
    """

    def __init__(self):
        self.output_dir = "Output"
        self.tags = THREADING_TAGS
        self.driverargs = []
        self.semaphore = threading.BoundedSemaphore(value=DEFAULT_THREADS)
        self.config_execution_data = {}
        self.global_argument_counter = 0
        self.parse_arguments()
        self.check_clean_output_directory()
        self.threads_to_run = []

    def parse_arguments(self):
        "Parses the arguments to execute tests in thread driver."
        parser = argparse.ArgumentParser("Robot Framework Thread Tests Driver")
        parser.add_argument(
            "-t",
            "--tags",
            help="Tags list to run in parallel through threads",
            default=self.tags,
            nargs="*",
        )
        parser.add_argument(
            "-nt",
            "--number_threads",
            type=int,
            help="Number of max threads to run in parallel.",
            default=DEFAULT_THREADS,
        )
        parser.add_argument(
            "-cf",
            "--config_execution_file",
            help="Yaml file describing config for reading parallel execution data.",
            default="",
        )
        args, self.driverargs = parser.parse_known_args(sys.argv[1:])

        self.tags = args.tags
        self.threads_numbers = (
            DEFAULT_THREADS if args.number_threads <= 0 else args.number_threads
        )
        self.semaphore = threading.BoundedSemaphore(value=self.threads_numbers)
        if args.config_execution_file:
            self._process_config_parallel_execution(args.config_execution_file)

    def _process_config_parallel_execution(self, config_execution_file):
        """
        Reads configs to run in parallel against the specified tags. This will describe how
        many tasks can run in parallel depending on the execution config requirements.
        Sets data inside self.config_execution_data variable.

        :param config_execution_file: Yaml config file
        """
        with open(config_execution_file, "r") as stream:
            try:
                self.config_execution_data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    def check_clean_output_directory(self):
        """
        Checks if clean output directory is set and cleans folder.
        """
        output_directory = self._get_driver_args("--output_dir", "-o")
        driver_object = driver.Driver(readSystemParams=False)
        driver_object.clean_output_directory(output_directory)
        if "--notcleanoutputdir" in self.driverargs:
            self.driverargs.remove("--notcleanoutputdir")
        if "-nc" in self.driverargs:
            self.driverargs.remove("--notcleanoutputdir")

    def _get_argument_name(self, arguments):
        """
        Fetches the tag name to be added to the log file in output results.
        Searches for argument for device name and returns that value adding
        a global counter value to it.

        :param arguments: Variables related to a particular device. type dict.
        :return: device name with a global incremented counter.
        """
        self.semaphore.acquire()
        self.global_argument_counter += 1
        self.semaphore.release()
        devicename = ""
        for argument in arguments:
            if "devicename" in argument.lower():
                devicename = arguments[argument]
                break

        return str(devicename) + "_" + str(self.global_argument_counter)

    def start_execution(self, tag="", argument_list=None, additional_tag_name=""):
        """
        Starts the thread execution using a bounded semaphore
        """
        self.semaphore.acquire()
        driver_obj = driver.Driver(readSystemParams=False, cleanoutputdir=False)
        driver_args = self.driverargs[:]

        if argument_list:
            variables = self._get_driver_args("--variables", "-vars", optiontype=list)
            orig_variable_list_length = len(variables)
            if variables:
                variables.extend(argument_list)
            if "-vars" in driver_args:
                index = driver_args.index("-vars") + 1
                driver_args[index : orig_variable_list_length + index] = variables
            elif "--variables" in self.driverargs:
                index = driver_args.index("--variables") + 1
                driver_args[index : orig_variable_list_length + index] = variables
            else:
                driver_args["-vars"] = argument_list

        driver_obj.parse_arguments(driver_args)
        self.output_dir = driver_obj.output_dir
        driver_obj.execute_tests(tag, additional_tag_name)
        self.semaphore.release()

    def process_config_threads(self):
        """
        This creates the required threads and adds them for execution into self.threads_to_run.
        """
        for instance in self.config_execution_data["instances"]:
            variable_list = []
            for variable in instance:
                variable_list.append(str(variable) + ":" + str(instance[variable]))
            for tag in self.tags:
                additional_tag_name = self._get_argument_name(instance)
                new_thread = threading.Thread(
                    target=self.start_execution,
                    args=(tag, variable_list, additional_tag_name),
                )
                self.threads_to_run.append(new_thread)

    def add_execute_threads(self):
        """
        Adds the threads to execute in self.thread_to_run variable
        """
        if self.config_execution_data:
            self.process_config_threads()
        else:
            for tag in self.tags:
                new_thread = threading.Thread(target=self.start_execution, args=(tag,))
                self.threads_to_run.append(new_thread)

    def start_threads(self):
        "Starts the threads execution and waits for completion."
        for thread_to_start in self.threads_to_run:
            thread_to_start.start()

        # Wait until all threads are completed
        for thread_to_join in self.threads_to_run:
            thread_to_join.join()

    def _get_driver_args(self, option1, option2, optiontype=str):
        "Fetches value from driver args"
        value = None
        parser = argparse.ArgumentParser()
        if optiontype == str:
            value = ""
            parser.add_argument(option1, option2, default=value)
        else:
            value = []
            parser.add_argument(option1, option2, default=value, nargs="*")
        args, _ = parser.parse_known_args(self.driverargs)
        if eval("args." + option1.lstrip("-")):
            value = eval("args." + option1.lstrip("-"))
        return value

    def get_combine_reports(self):
        "Combines the report generated from the threads into one"
        output_directory = self._get_driver_args("--output_dir", "-o")
        if output_directory:
            driver_object = driver.Driver(
                readSystemParams=False, output_dir=output_directory
            )
        else:
            driver_object = driver.Driver(readSystemParams=False)
        driver_object.combine_reports()

    def execute_and_report(self):
        "Merges the results to result in one output file."
        self.add_execute_threads()
        self.start_threads()
        self.get_combine_reports()


if __name__ == "__main__":
    THREAD_DRIVER_OBJ = ThreadDriver()
    THREAD_DRIVER_OBJ.execute_and_report()
