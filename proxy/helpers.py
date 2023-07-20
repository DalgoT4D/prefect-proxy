import inspect
import logging
import os
import re


class CustomLogger:
    """override the python logger to include the orgname"""

    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

    def get_org_slug(self):
        """
        Get the value of the 'x-ddp-org' header from
        calling frame's 'request' object
        """
        try:
            stack = inspect.stack()
            for frame_info in stack:
                frame = frame_info.frame
                request = frame.f_locals.get("request")
                if request is not None and hasattr(request, "headers"):
                    org_slug = request.headers.get("x-ddp-org")
                    if org_slug is not None:
                        return org_slug
        except Exception as error:
            self.logger.error("An error occurred while getting slug: %s", str(error))
        return ""

    def info(self, *args):
        """Override the logger.info method to include the orgname"""
        orgname = self.get_org_slug()
        caller_name = inspect.stack()[1].function
        file_path = inspect.stack()[1].filename
        file_name = os.path.basename(file_path)
        self.logger.info(
            *args,
            extra={
                "caller_name": caller_name,
                "orgname": orgname,
                "file_name": file_name,
            }
        )

    def error(self, *args):
        """call logger.error with the caller_name and the orgname"""
        orgname = self.get_org_slug()
        caller_name = inspect.stack()[1].function
        file_path = inspect.stack()[1].filename
        file_name = os.path.basename(file_path)
        self.logger.error(
            *args,
            extra={
                "caller_name": caller_name,
                "orgname": orgname,
                "file_name": file_name,
            }
        )

    def exception(self, *args):
        """call logger.exception with the caller_name and the orgname"""
        orgname = self.get_org_slug()
        caller_name = inspect.stack()[1].function
        file_path = inspect.stack()[1].filename
        file_name = os.path.basename(file_path)
        self.logger.exception(
            *args,
            extra={
                "caller_name": caller_name,
                "orgname": orgname,
                "file_name": file_name,
            }
        )


def cleaned_name_for_prefectblock(blockname):
    """removes characters which are not lowercase letters, digits or dashes"""
    pattern = re.compile(r"[^\-a-z0-9]")
    return re.sub(pattern, "", blockname.lower())
