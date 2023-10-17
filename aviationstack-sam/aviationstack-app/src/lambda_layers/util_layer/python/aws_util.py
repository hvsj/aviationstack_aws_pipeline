from . import *


def get_aws_logger(level_name: str):
    """
    :param level_name: Log Level Name to log messages at different severity levels (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :type: str
    :return: It returns a logger
    """
    # Create and configure logger
    logger = logging.getLogger()
    level = logging.getLevelName(level_name)
    logger.setLevel(level)
    return logger


def get_ssm_param(parameter_name, region_name: str, **kwargs):
    """
        for more details about response if parameter found in SSM parameter store
        follow https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm/client/get_parameter.html

    :param parameter_name:
    :type: str
    :param region_name:
    :type: str
    :param kwargs:
    :return: returns parameter value if found else raises an error
    :rtype: str
    """
    try:
        if 'profile_name' in kwargs:
            session = boto3.session.Session(profile_name=kwargs['profile_name'])
            ssm_client = session.client(service_name='ssm', region_name=region_name)
        else:
            ssm_client = boto3.client(service_name='ssm', region_name=region_name)

        response = ssm_client.get_parameter(Name=parameter_name)

    except ClientError as e:
        if e.response['Error']['Code'] == 'InternalServerError':
            print("An error occurred on the server side.")
            raise e
        if e.response['Error']['Code'] == 'InvalidKeyId':
            print("You provided a parameter key that is not valid/not found.")
            raise e
        if e.response['Error']['Code'] == 'ParameterNotFound':
            print("SSM Parameter Store can't find provided key.")
            raise e
        if e.response['Error']['Code'] == 'ParameterVersionNotFound':
            print("SM Parameter Store can't find provided key version.")
            raise e
    except Exception as e:
        print(e)
        raise e

    else:
        param_value = functools.reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, ['Parameter', 'Value'], response)
        return param_value


def remove_file_from_s3(logger, file_name: str, bkt_name: str, **kwargs):
    try:
        if 'profile_name' in kwargs:
            my_session = boto3.session.Session(profile_name=kwargs['profile_name'])
            s3 = my_session.client('s3')
        else:
            s3 = boto3.client('s3')
        response = s3.delete_object(Bucket=bkt_name, Key=file_name)
        logger.info(response)
        return True
    except ClientError as e:
        logger.error(e)
        return False


def upload_from_s3_to_lambda(bkt_name: str, file_path: str, temp_fp: str, **kwargs) -> bool:
    """
    downloading file from s3 to lambda local folder
    :param bkt_name: name of the bucket in which file is present
    :type: str
    :param file_path: entire file path
    :type: str
    :param temp_fp: lambda temporary local folder. Generally its constant ->/tmp/<file_name>
    :type: str
    :return: returns either True or False
    :rtype: bool
    """
    try:
        if 'profile_name' in kwargs:
            my_session = boto3.session.Session(profile_name=kwargs['profile_name'])
            s3 = my_session.client('s3')
        else:
            s3 = boto3.client('s3')
        s3.meta.client.download_file(bkt_name, file_path, temp_fp)
        return True
    except ClientError as e:
        print(e)
        return False


def upload_from_lambda_to_s3(bkt_name: str, file_path: str, temp_fp: str, **kwargs) -> bool:
    """
    downloading file from s3 to lambda local folder
    :param bkt_name: name of the bucket in which file is present
    :type: str
    :param file_path: entire file path
    :type: str
    :param temp_fp: lambda temp
    :type: str
    :return: returns either True or False
    :rtype: bool
    """
    try:
        if 'profile_name' in kwargs:
            my_session = boto3.session.Session(profile_name=kwargs['profile_name'])
            s3 = my_session.client('s3')
        else:
            s3 = boto3.client('s3')
        s3.meta.client.upload_file(temp_fp, bkt_name, file_path)
        return True
    except ClientError as e:
        print(e)
        return False


def upload_file_like_obj_to_s3(logger, file_name: str, bkt_name: str, src_sub_folder: str, obj, **kwargs) -> bool:
    """
        uploading file from local to s3 bucket
    :param src_sub_folder:
    :param logger:
    :param file_name: name of the file
    :type: str
    :param bkt_name: name of the bucket in which file needs to be uploaded
    :type: str
    :param obj: bytes or seekable file-like object
    :type: str
    :return: returns either True or False
    :rtype: bool
    """
    try:
        if 'profile_name' in kwargs:
            my_session = boto3.session.Session(profile_name=kwargs['profile_name'])
            s3 = my_session.client('s3')
        else:
            s3 = boto3.client('s3')

        s3.put_object(Body=json.dumps(obj), Bucket=bkt_name, Key=src_sub_folder + file_name)
        # s3.put_object(Body=json.dumps(obj), Bucket=bkt_name + src_sub_folder, Key=file_name)
        return True
    except ClientError as e:
        logger.error(e)
        return False
    except Exception as e:
        logger.error(e)
        return False


def get_secret(logger, secret_name: str, region_name: str, **kwargs):
    try:
        if 'profile_name' in kwargs:
            session = boto3.session.Session(profile_name=kwargs['profile_name'])
            sm_client = session.client(service_name='secretsmanager', region_name=region_name)
        else:
            sm_client = boto3.client(service_name='secretsmanager', region_name=region_name)

        get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            logger.error("Secrets Manager can't decrypt the protected secret text using the provided KMS key.")
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            logger.error("An error occurred on the server side.")
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            logger.error("You provided an invalid value for a parameter.")
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            logger.error("You provided a parameter value that is not valid for the current state of the resource.")
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error("We can't find the resource that you asked for.")
            raise e
    else:
        logger.info("Decrypting secret using the associated KMS key.")
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(decoded_binary_secret)
