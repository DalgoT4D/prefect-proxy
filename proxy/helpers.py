import inspect
import logging
import re


class CustomLogger:
    """override the python logger to include the orgname associated with every request"""
    
    def __init__(self, name, level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

    def get_org_slug(self):
        """Get the value of the 'org_slug' variable"""
        try:
            stack = inspect.stack()
            for frame_info in stack:
                frame = frame_info.frame
                userorg = frame.f_locals.get("org_slug")
                if userorg is not None:
                    return userorg
        except Exception as error:
            self.logger.error("An error occurred while getting slug: %s", str(error))
        return ""

    def info(self, *args):
        """Override the logger.info method to include the orgname"""
        orgname = self.get_org_slug()
        caller_name = inspect.stack()[1].function
        self.logger.info(*args, extra={"caller_name": caller_name, "orgname": orgname})

    def error(self, *args):
        """call logger.error with the caller_name and the orgname"""
        orgname = self.get_org_slug()
        caller_name = inspect.stack()[1].function
        self.logger.error(*args, extra={"caller_name": caller_name, "orgname": orgname})

    def exception(self, *args):
        """call logger.exception with the caller_name and the orgname"""
        orgname = self.get_org_slug()
        caller_name = inspect.stack()[1].function
        self.logger.exception(
            *args, extra={"caller_name": caller_name, "orgname": orgname}
        )


def cleaned_name_for_prefectblock(blockname):
    """removes characters which are not lowercase letters, digits or dashes"""
    pattern = re.compile(r"[^\-a-z0-9]")
    return re.sub(pattern, "", blockname.lower())
