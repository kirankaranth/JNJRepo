"""
Generates pdf report through html file generation for attaching to the test execution report.
"""
import html
import mimetypes
import ntpath
import os
import jinja2
import pdfgen
import variables
import xray_logger_handler
from date_and_time_support import DateTimeSupport

# variables
SECONDS = " seconds"


class ReportGenerator:
    """
    Handles generation of html report and through it pdf report generation.
    """

    def __init__(self, test, jira_test, args):
        self.test = test
        self.jira_test = jira_test
        self.args = args
        self.logger = xray_logger_handler.setup()
        self.date_time_support = DateTimeSupport(self.args)
        self.report_version = variables.REPORT_VERSION
        parent_dir = ntpath.dirname(self.args.results_file)
        self.report_directory = os.sep.join(
            [parent_dir, variables.REPORT_OUTPUT_FOLDER]
        )
        self._setup_report_output_directory()

    def generate_all_pdf_reports(self):
        """
        Sets up self.data_dict from self.suites data for html rendering
        """
        self._escape_test_values(self.test, self._escape_html)
        test_dict = self._setup_test_dict_data(self.test)
        report_name = self.generate_pdf_report(test_dict)
        self._escape_test_values(self.test, self._unescape_html, actual_result=True)
        if self.test.attachment is None:
            self.test.attachment = [report_name]
        else:
            self.test.attachment.append(report_name)

    def _setup_report_output_directory(self):
        """
        Deletes self.report_directory if it exists
        """
        if not os.path.exists(self.report_directory):
            os.makedirs(self.report_directory)

    @staticmethod
    def _escape_html(value):
        """
        Escapes html string
        :param value: String value to escape for html representation
        :return: escaped html value
        """
        return html.escape(value)

    @staticmethod
    def _unescape_html(value):
        """
        Escapes html string
        :param value: String value to escape for html representation
        :return: escaped html value
        """
        return html.unescape(value)

    @staticmethod
    def _get_template_object():
        """
        Fetches the template object to render
        :return: Template object
        """
        dir_path = os.path.dirname(os.path.realpath(__file__))
        template_path = os.sep.join([dir_path, "Templates"]) + os.sep
        template_loader = jinja2.FileSystemLoader(searchpath=template_path)
        template_env = jinja2.Environment(loader=template_loader, autoescape=True)
        template = template_env.get_template("template.html")
        return template

    def _get_times(self, start_time, end_time):
        """
        Converts time of test string representation and calculate total time in seconds
        :param start_time: Start time of execution
        :param end_time: End time of execution
        :return: start, end and total time tuple in string format
        """
        start_time = self.date_time_support.get_datetime_for_json(start_time)
        end_time = self.date_time_support.get_datetime_for_json(end_time)
        total_time = str((end_time - start_time).total_seconds()) + SECONDS

        time_format = "%a %d %b %Y at %T:%f"
        start_time = (
            start_time.strftime(time_format) + self.date_time_support.get_timezone_local
        )
        end_time = (
            end_time.strftime(time_format) + self.date_time_support.get_timezone_local
        )
        return start_time, end_time, total_time

    @staticmethod
    def _search_filepath_under_parent_directory(parent_path, attachment_path):
        """
        Checks for presence of attachment path under the parent path.
        :param parent_path: Path under which a file subpath to be searched
        :param attachment_path: Attachment path to search for presence
        :return: Complete path of an existing file or attachment_path
        """
        path_found = attachment_path
        orig_file_name = ntpath.basename(attachment_path)
        for root, dirs, files in os.walk(parent_path):
            files_found = [
                file_name for file_name in files if file_name == orig_file_name
            ]
            for file_found in files_found:
                complete_file = os.path.join(root, file_found)
                if complete_file.endswith(attachment_path):
                    path_found = complete_file
                    break
            if files_found:
                break
        return path_found

    def _get_attachment_file_path(self, parent_path, attachment_path):
        """
        Gets attachment file path which is verified to exist.
        :param parent_path: Output folder where the test results are added
        :param attachment_path: Path specified in the xml file
        :return: Actual path where the attachment file exists
        """
        path = attachment_path
        if not os.path.isabs(attachment_path):
            test_path = os.sep.join([parent_path, attachment_path])
            if os.path.exists(test_path):
                path = test_path
            else:
                path = self._search_filepath_under_parent_directory(parent_path, path)
        return path

    def _get_attachment_information_list(self, attachments):
        """
        Processes the attachments to build a dictionary of data for attachment information
        :param attachments: Attachment to be read for metadata
        :return: List of attachments for the particular execution point
        """ ""
        attachment_list = []
        parent_path = os.path.abspath(os.path.join(self.args.results_file, os.pardir))
        for counter, attachment in enumerate(attachments):
            path = self._get_attachment_file_path(
                parent_path, attachment.attachment_path
            )
            attachments[counter].attachment_path = path
            content_type = mimetypes.guess_type(path)
            attachment_type = "link"
            if content_type[0].startswith("image"):
                attachment_type = "image"
            modification_time = attachment.time_created
            modification_time = modification_time.strftime("%a %b %d %Y at %H:%M:%S")
            attachment_dict = {
                "path": path,
                "title": ntpath.basename(path),
                "content_type": attachment_type,
                "last_modification": modification_time,
            }
            attachment_list.append(attachment_dict)
        return attachment_list

    def _process_steps_data(self, counter, step, step_dict):
        """
        Creates information for html rendering for step in step_dict
        :param counter: Number of step being built
        :param step: Step object processed
        :param step_dict: dictionary to be populated with information
        """
        step_dict.setdefault(counter, {})
        step_dict[counter]["step"] = step
        total_time = str((step.end_time - step.start_time).total_seconds()) + SECONDS
        step_dict[counter][
            "start_time"
        ] = self.date_time_support.get_time_format_string(step.start_time)
        step_dict[counter]["end_time"] = self.date_time_support.get_time_format_string(
            step.end_time
        )
        step_dict[counter]["total_time"] = total_time
        attachment_step = self._get_attachment_information_list(step.attachment)
        step_dict[counter]["attachment"] = attachment_step
        step_dict[counter]["execution_counter"] = step.execution_counter

    def _setup_test_dict_data(self, test):
        """
        Builds dictionary data for processing test data for pdf report
        :param test: Test object read from report file xml/json format
        :return: Test data dictionary
        """
        test_dict = {}
        test_dict["test"] = test
        test_dict["tester"] = self.args.username
        test_dict["report_version"] = variables.REPORT_VERSION
        test_dict["test_execution_id"] = self.args.xray_testexecution_id
        test_dict["story_jira_id"] = self._get_list_format(test.jira_id)
        test_dict["test_jira_id"] = self.jira_test.key
        total_time = str((test.end_time - test.start_time).total_seconds()) + SECONDS
        test_dict["start_time"] = self.date_time_support.get_time_format_string(
            test.start_time
        )
        test_dict["end_time"] = self.date_time_support.get_time_format_string(
            test.end_time
        )
        test_dict["total_time"] = total_time
        test_dict["attachment"] = self._get_attachment_information_list(test.attachment)
        test_dict["test_type"] = self.args.test_type
        steps_dict = {}
        for step_counter, step in enumerate(test.steps):
            self._process_steps_data(step_counter + 1, step, steps_dict)
        test_dict["steps"] = steps_dict
        return test_dict

    def _escape_test_values(self, test, escape_function, actual_result=False):
        """
        Escapes test values for html representation
        :param test: Test object to be represented in html
        :param escape_function: Function to be used for escaping value
        :param actual_result: Escapes actual value for result creation
        """
        test.doc = escape_function(test.doc)
        test.label_id = [escape_function(label) for label in test.label_id]
        test.name = escape_function(test.name)
        test.os = escape_function(test.test_os)
        test.test_tags = [escape_function(tag) for tag in test.test_tags]
        self._escape_variables(test.variables, "Test", escape_function)
        for counter, step in enumerate(test.steps):
            test.steps[counter] = self._escape_step_values(
                step, escape_function, actual_result
            )

    def _escape_step_values(self, step, escape_function, actual_result):
        """
        Escapes step values for html representation
        :param step: Step object to be represented in html
        :param escape_function: Escape function to be applied to code
        :param actual_result: Boolean to escape or escape actual value or not
        :return: Step object with escaped values inside step object for special html characters
        """
        step.actual = escape_function(step.actual)
        if actual_result:
            # The curly brackets though not html escaped need to be escaped here
            step.actual = step.actual.replace("{", "&#123;").replace("}", "&#125;")
        step.description = escape_function(step.description)
        step.expected = escape_function(step.expected)
        self._escape_variables(step.variables, "Step", escape_function)
        return step

    def _escape_variables(self, variable_dict, variable_level, escape_function):
        """
        Escapes variable dict key, value for html representation
        :param variable_dict: Variable dict object to be represented in html
        :param variable_level: Level where the values are being escaped from Test/Step
        :param escape_function: Escape function to be applied to code
        """
        update_dict = variable_dict
        for key in variable_dict:
            if variable_level == "Step":
                update_dict = variable_dict[key]
            self._escape_key_at_dict_level(update_dict, "USER", escape_function)
            self._escape_key_at_dict_level(update_dict, "ROBOT", escape_function)
            self._escape_key_at_dict_level(update_dict, "STRING", escape_function)
            if variable_level == "Test":
                break

    @staticmethod
    def _escape_key_at_dict_level(variable_dict, level, escape_function):
        """
        Escapes variable in variable_dict identified at level
        :param variable_dict: Dict of type {"USER":, "ROBOT":, "STRING": }
        :param level: Level at which value is to be escaped
        :param escape_function: Escape function to be applied to code
        """
        if level not in variable_dict:
            return
        new_level_dict = {}
        if level in ["USER", "ROBOT"]:
            for user_variable in variable_dict.get(level, {}):
                escaped_variable = escape_function(user_variable)
                new_level_dict[escaped_variable] = escape_function(
                    variable_dict[level][user_variable]
                )
            variable_dict[level] = new_level_dict
        elif level == "STRING":
            new_level_dict = escape_function(variable_dict[level])
        variable_dict[level] = new_level_dict

    @staticmethod
    def _get_list_format(elements):
        """
        Gets comma separated list for elements if they are in type list
        :param elements: Elements to read for string representation
        :return: String value to be logged into html report
        """
        if isinstance(elements, list):
            elements = ", ".join(elements)
        return elements

    def _create_html_format(self, test_dict):
        """
        Creates html rendered template string from data read during initialization
        :param test_dict: Dict of data used to generate html using jinja templates
        :return: html rendered template string
        """
        template = self._get_template_object()
        return template.render(test_dict)

    def _create_pdf_format(self, file_path, pdf_file_name):
        """
        Creates mht file from the passed html file.
        :param file_path: html file path to be converted to type mht
        :param pdf_file_name: PDF file name to be generated from this test
        :return: Created pdf file path
        """
        pdf_file = os.sep.join([self.report_directory, pdf_file_name])
        pdfgen.sync.from_file(file_path, pdf_file)
        return pdf_file

    def generate_pdf_report(self, test_dict):
        """
        Generates pdf report by first generating an html report and then converting
         it to pdf
        """
        file_data = self._create_html_format(test_dict)
        html_report_name = self.jira_test.key + "_" + "Test_Report.html"
        file_path = os.sep.join([self.report_directory, html_report_name])
        with open(file_path, "w") as file_object:
            file_object.write(file_data)
        pdf_file_name = html_report_name.replace(".html", ".pdf")
        return self._create_pdf_format(file_path, pdf_file_name)
