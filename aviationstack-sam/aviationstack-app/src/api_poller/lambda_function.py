# ----- user-defined modules
from snowflake_util import connect_to_snowflake
from common_util import main, functools, json
from aws_util import get_aws_logger, get_secret, get_ssm_param


ENV = 'DEV'
REGION_NAME = 'us-east-1'

SECRET_NAME = get_ssm_param(parameter_name=f'/{ENV}/secrets_arn', region_name=REGION_NAME)
LOG_LEVEL = get_ssm_param(parameter_name=f'/{ENV}/log_level_name', region_name=REGION_NAME)

# ------------------------- LOGGING DETAILS
LOGGER = get_aws_logger(level_name=LOG_LEVEL)

CONFIG_DICT: dict = get_secret(logger=LOGGER, secret_name=SECRET_NAME, region_name=REGION_NAME)


# ------------------------- SNOWFLAKE DETAILS
SF_CREDENTIALS = functools.reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, ['SNOWFLAKE_DETAILS', 'SNOWFLAKE_CREDENTIALS'], CONFIG_DICT)

# Connect to snowflake
if not SF_CREDENTIALS:
    raise Exception("Unable to find Snowflake credentials in configuration file!!!")

bool_value, msg, sf_ctx = connect_to_snowflake(logger=LOGGER, credentials=SF_CREDENTIALS)
if not bool_value:
    raise Exception(msg)

API_TOPIC = 'countries'


def lambda_handler(event, context):
    logger = LOGGER
    bkt_name = functools.reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
                      ['S3_DETAILS', 'BUCKET_DETAILS', 'countries', 'path'], CONFIG_DICT)
    sub_folder = functools.reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
                        ['S3_DETAILS', 'BUCKET_DETAILS', 'countries', 'sub_folder'], CONFIG_DICT)
    file_name = functools.reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None,
                       ['S3_DETAILS', 'BUCKET_DETAILS', 'countries', 'file_name'], CONFIG_DICT)

    logger.info(f"file_name: {file_name}, bucket_name: {bkt_name}, sub_folder: {sub_folder}")

    if not bkt_name or not file_name or not sub_folder:
        logger.info("One of the bkt_name or sub_folder or file_name is not available in config file!!!")
        raise Exception("One of the bkt_name or sub_folder or file_name is not available in config file!!!")

    main(logger=logger, sf_ctx=sf_ctx, file_name=file_name, bkt_name=bkt_name, sub_folder=sub_folder,
         api_topic=API_TOPIC)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

