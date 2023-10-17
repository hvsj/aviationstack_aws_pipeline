from . import logging, datetime


def get_timestamp(timezone: str, timestamp_fmt: str):
    """
        This function uses built zone info library if application uses python 3.9+ version
        and return timestamp in required format at given timezone

        If application uses python version older than 3.9+ version, it recommends you to download dateutil library
        and uncomment the code given to get the timestamp for required timezone
    :param timezone: IANA timezone
    :param timestamp_fmt: required timestamp format
    :return: It returns timestamp in required format at given timezone
    """
    import sys
    if sys.version_info >= (3, 9):
        print("Python version is 3.9 or higher, so we can use zoneinfo")
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(timezone)
            timestamp = datetime.now(tz=tz).strftime(timestamp_fmt)
            return timestamp
        except Exception as e:
            print(e)
            raise Exception(e)
    else:
        print("Hi, current Python version is not compatible with zone info.")
        print("If timezone required, try installing dateutil module in your application")
        print("Uncomment the below code once dateutil is installed")
        print("Else use this function with default timezone, Thank you!")

        # try:
        #     from dateutil import tz
        #
        #     t_zone = tz.gettz(timezone)
        #     timestamp = datetime.now(tz=t_zone).strftime(timestamp_fmt)
        #     return timestamp
        # except Exception as e:
        #     print(e)
        #     raise Exception(e)


def get_logger(log_config_dict: dict, log_filepath, **kwargs):
    """
    This function creates a logger to log all the required details in our application by taking
        1)  below config dict format
        2)  Location of log folder
        3)  Timezone if required

        config dict format:
            {
                ...

                "LOG_DETAILS": {
                    "LEVEL_NAME": "<<log_level>>",
                    "FILE_PATH": "",
                    "LOG_CONFIG": {
                        "filename": "<<log_filename>>",
                        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                        "filemode": "w",
                        "datefmt": "%d-%b-%y %H:%M:%S"
                    }
                }

                ...
            }

    **kwargs
        If user required timestamp to be certain timezone compliant,
        then he can pass IANA timezone as value to the key timezone.

            Example: without additional **kwarg
                get_logger(log_config_dict=log_config_dict, log_filepath=log_filepath)

            Example: with additional **kwarg which is timezone
                get_logger(log_config_dict=log_config_dict, log_filepath=log_filepath, timezone='America/New_York')

    :param log_config_dict: A log configuration dict which looks like mentioned above
    :param log_filepath: Location of log folder, where log files need to be saved
    :param kwargs: Currently this function expect timezone, timestamp_fmt as **kwarg
    :return: It returns a logger
    """
    #   Checking if Log file timestamp format - timestamp_fmt argument is available in **kwargs or not
    if 'timestamp_fmt' in kwargs:
        log_file_timestamp_fmt = kwargs['timestamp_fmt']
    else:
        log_file_timestamp_fmt = "%Y%m%d_%H%M%S"

    #   Checking if timezone argument is available in **kwargs or not
    if 'timezone' in kwargs:
        timestamp = get_timestamp(timezone=kwargs['timezone'], timestamp_fmt=log_file_timestamp_fmt)
    else:
        timestamp = datetime.now().strftime(log_file_timestamp_fmt)

    # Create and configure logger
    level_name = log_config_dict['LOG_DETAILS']['LEVEL_NAME']
    log_config_dict['LOG_DETAILS']['LOG_CONFIG']['filename'] = log_filepath / str(
        log_config_dict['LOG_DETAILS']['LOG_CONFIG']['filename']).format(timestamp=timestamp)

    log_config = log_config_dict['LOG_DETAILS']['LOG_CONFIG']
    logging.basicConfig(**log_config)

    # Creating an object
    logger = logging.getLogger()

    level = logging.getLevelName(level_name)
    logger.setLevel(level)
    return logger
