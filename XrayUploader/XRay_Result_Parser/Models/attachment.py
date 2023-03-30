"""
Defines Attachment structure object for Robot Framework test report for Jira XRay
"""


class Attachment:
    """
    Attachment structure read through robot report
    """

    def __init__(self, attachment_path, time_created):
        self.attachment_path = attachment_path
        self.time_created = time_created
