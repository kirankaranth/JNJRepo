"""
Fetches images from a Keyword.
"""
import urllib
from datetime import datetime
from .times_support import TimeSupport
from ..Models.attachment import Attachment


class Images:
    """
    Finds images under a keyword looking through recursively under a keyword
    """

    def __init__(self, soup_support):
        """
        Initialized by a keyword to look under recusively.
        :param keyword: Keyword of type bs4.Element.Tag type to look under images in messages
        """
        self.soup_support = soup_support
        self.times_support = TimeSupport(soup_support)

    def get_images(self, keyword, recursive=True):
        """
        Fetches images under the keyword
        :param keyword: XML element under which to look for images
        :param recursive: Boolean to look for images recursively below a tag
        :return: list of images read from the keyword
        """
        messages = self.soup_support.read_all_messages(keyword, recursive=recursive)
        images = []
        for msg in messages:
            image_time_list = self._read_image_data_from_message(msg)
            if image_time_list:
                images.append(image_time_list)
        return self._get_images_object_list(images)

    def _read_image_data_from_message(self, message):
        """
        Reads and return image data from message tag
        :param message: Message tag which may contain image information
        :return: Image path
        """
        image_time_list = []
        if message.string and 'img src="' in message.string:
            image = message.string.split('img src="', 1)[1].split('"', 1)[0]
            image = urllib.parse.unquote(image)
            time_added = self._get_time_added(message)
            image_time_list = [image, time_added]
        return image_time_list

    def _get_time_added(self, message_tag):
        """
        Gets the time that a message tag was recorded. Keeps looking up
        recursively for a status tag sibling until it finds one or finds the
        test tag parent suite.
        :param message_tag: Message tag containing an image
        :return: Time when the particular step was executed to take screenshot
        """
        parent_tag = message_tag.parent
        status = None
        message_time = datetime.now()
        while not status and parent_tag.name != "suite":
            status = message_tag.parent.findChildren("status", recursive=False)
            if not status:
                parent_tag = parent_tag.parent
        if status:
            time_and_status_dict = self.times_support.get_times_and_status(parent_tag)
            message_time = time_and_status_dict["end_time"]
        return message_time

    @staticmethod
    def _get_images_object_list(images_list):
        """
        Creates attachment object from images list
        :param images_list: List of image of format [[image_path, time_image_taken]..]
        :return: attachment object list
        """
        attachment_objects = []
        for item in images_list:
            attachment_object = Attachment(item[0], item[1])
            attachment_objects.append(attachment_object)
        return attachment_objects
