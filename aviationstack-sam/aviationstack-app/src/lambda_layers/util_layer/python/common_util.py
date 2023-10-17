from . import *


def timer_decorator(func):
    @functools.wraps(func)
    def timer_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return timer_wrapper


def revert_pagination_updates(logger, sf_ctx, params: dict, api_topic: str):
    old_offset = params['offset']
    query = f"UPDATE AVIATION_DB.CNFG.CNFG_API_DETAILS SET PARAMS = OBJECT_INSERT(PARAMS, 'offset', {old_offset}, TRUE) WHERE API_TOPIC='{api_topic}';"
    bool_value, msg, sf_query_id = execute_query(logger=logger, sf_ctx=sf_ctx, query=query)
    if not bool_value:
        return False, msg, sf_query_id
    return True, msg, sf_query_id


def update_pagination(logger, sf_ctx, pagination: dict, api_topic: str):
    next_offset = pagination['offset']
    query = f"UPDATE AVIATION_DB.CNFG.CNFG_API_DETAILS SET PARAMS = OBJECT_INSERT(PARAMS, 'offset', {next_offset}, TRUE) WHERE API_TOPIC='{api_topic}';"
    bool_value, msg, sf_query_id = execute_query(logger=logger, sf_ctx=sf_ctx, query=query)
    if not bool_value:
        return False, msg, sf_query_id
    return True, msg, sf_query_id


def check_pagination(logger, pagination: dict):
    # "pagination": {
    #     "limit": 100,
    #     "offset": 0,
    #     "count": 100,
    #     "total": 9368
    # }
    offset = pagination.get("offset", None)
    count = pagination.get("count", None)
    total = pagination.get("total", None)

    if offset is None or count is None or total is None:
        logger.info(f"offset: {offset}, count: {count}, total: {total}")
        return False, "One of the offset or count or total is missing!!!", None

    if offset == 0:
        return True, "API is running for the first time!!!", 1
    elif 0 < offset < total:
        return True, "API can fetch data from Endpoint!!!", 1
    elif offset < 0:
        return False, "Offset cannot be negative, there might be some configuration issue!!!", 0
    else:
        return True, "API fetched all the data from the Endpoint, Hence aborting API GET request!!!", None


def get_pagination(response: dict):
    pagination = response.get("pagination", None)
    if not pagination:
        if response:
            return False, "Could not find pagination in the given response!!!", None
        return False, "Response is empty, hence can't find pagination!!!", None
    return True, "Pagination found in the given response!!!", pagination


def get_request(logger, url: str, params: dict):
    response = requests.get(url=url, params=params)

    logger.info(str(response.url))

    if response.status_code == 200:
        logger.info("Request was successful!")
        return True, "Request was successful!", response.json()
    else:
        return False, f"Request failed with status code: {response.status_code}", {}


def get_api_details(logger, sf_ctx, api_topic: str):
    cur = sf_ctx.cursor(sf.DictCursor)
    try:
        query = f"SELECT * FROM AVIATION_DB.CNFG.CNFG_API_DETAILS WHERE API_TOPIC='{api_topic}';"
        logger.info(f"QUERY: {query}")
        result = cur.execute(query).fetchone()

        if not result:
            raise Exception("Result is None!!!")

        parameters: dict = {
            "access_key": result["API_ACCESS_KEY"]
        }
        parameters.update(json.loads(result["PARAMS"]))
        url = result["API_ENDPOINT"]

        # args_list = json.loads(result["ARGS_LIST"])
        # for r in args_list:
        #     url = '{}/{}'.format(url, r)

        if parameters:
            if sys.version_info >= (3, 0):
                import urllib.parse
                url = '{}?{}'.format(url, urllib.parse.urlencode(parameters))
            else:
                import urllib
                url = '{}?{}'.format(url, urllib.urlencode(parameters))

        return True, "Successfully able to retrieve API details!!!", url, parameters

    except sf.errors.ProgrammingError as e:
        logger.error(e)
        logger.info(str(traceback.format_exc()))
        logger.error('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return False, "Unable to execute given query, hence couldn't retrieve API details!!!", None, None
    except Exception as e:
        logger.error(e)
        logger.info(str(traceback.format_exc()))
        return False, "Unable to retrieve API Details!!!", None, None


def fetch_api_data(logger, sf_ctx, api_topic, file_name, bkt_name, sub_folder, profile_name):
    bool_value, msg, url, params = get_api_details(logger=logger, sf_ctx=sf_ctx, api_topic=api_topic)
    if not bool_value:
        logger.error(msg)
        # assert (event_id is not None), "Event id is None"
        raise Exception(msg)

    bool_value, msg, response = get_request(logger=logger, url=url, params=params)
    if not bool_value:
        logger.error(msg)
        raise Exception(msg)

    if not response:
        msg = "Response is empty please check the API and logs!!!"
        logger.info(msg)
        raise Exception(msg)

    bool_value, msg, pagination = get_pagination(response=response)
    if not bool_value:
        logger.error(msg)
        raise Exception(msg)

    bool_value, msg, value = check_pagination(logger=logger, pagination=pagination)
    if not bool_value:
        logger.error(msg)
        raise Exception(msg)
    if value is None:
        logger.info(msg)
        return

    data = response.get('data', None)

    if not data:
        msg = "Could not find data from the API, please check the logs!!!"
        logger.error(msg)
        raise Exception(msg)

    bool_value, msg, sf_query_id = update_pagination(logger=logger, sf_ctx=sf_ctx, pagination=pagination,
                                                     api_topic=api_topic)
    if not bool_value:
        logger.error("Unable to update pagination metadata, please check the details in logs!!!")
        logger.info(f"Update pagination Query id: {sf_query_id}")
        logger.error(msg)
        raise Exception(msg)

    bool_value = upload_file_like_obj_to_s3(logger=logger, file_name=file_name, bkt_name=bkt_name,
                                            src_sub_folder=sub_folder, obj=data, profile_name=profile_name)
    if not bool_value:
        msg = "Unable to upload file to S3 bucket, please check the logs!!!"
        logger.error(msg)
        # if we are unable to upload file into s3 bucket, then we need to revert back the metadata updates
        bool_value, msg, sf_query_id = revert_pagination_updates(logger=logger, sf_ctx=sf_ctx, params=params,
                                                                 api_topic=api_topic)
        if not bool_value:
            logger.error("Unable to update pagination metadata back to original before process start, please check the details in logs!!!")
            logger.info(f"Update pagination Query id: {sf_query_id}")
            logger.critical(f"Manually update pagination offset back to -> {params['offset']}")
        raise Exception(msg)


def main(logger, sf_ctx, api_topic: str, file_name: str, bkt_name: str, sub_folder, profile_name, **kwargs):
    #   Checking if file timestamp format - timestamp_fmt argument is available in **kwargs or not
    if 'timestamp_fmt' in kwargs:
        file_timestamp_fmt = kwargs['timestamp_fmt']
    else:
        file_timestamp_fmt = "%Y%m%d_%H%M%S"

    #   Checking if timezone argument is available in **kwargs or not
    if 'timezone' in kwargs:
        timestamp = get_timestamp(timezone=kwargs['timezone'], timestamp_fmt=file_timestamp_fmt)
    else:
        timestamp = datetime.now().strftime(file_timestamp_fmt)

    file_name = file_name.format(timestamp=timestamp)

    fetch_api_data(logger=logger, sf_ctx=sf_ctx, api_topic=api_topic, file_name=file_name, bkt_name=bkt_name,
                   sub_folder=sub_folder, profile_name=profile_name)

