"This class listens on test status from robot tests and accommodates for False Negatives"


class ResultsListener(object):
    "Listens on results from robot tests."
    ROBOT_LISTENER_API_VERSION = 3

    def end_test(self, _, test):
        """
        If at test end the staus is Fail but that was expected then this
        listener method passes it and vice versa
        """
        if "negative" in test.tags:
            if test.status == "FAIL":
                test.status = "PASS"
                test.message = "Test was expected to fail and it did fail."
            elif test.status == "PASS":
                test.status = "FAIL"
                test.message = "Test was expected to fail but it actually passed."
