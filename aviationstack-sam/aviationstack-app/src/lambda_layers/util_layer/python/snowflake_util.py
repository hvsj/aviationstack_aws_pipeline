from . import traceback, pd, sf


def connect_to_snowflake(logger, credentials: dict):
    try:
        sf_ctx = sf.connect(**credentials)
        return True, "Successfully connected to Snowflake!!", sf_ctx
    except Exception as e:
        logger.error(e)
        logger.info(str(traceback.format_exc()))
        return False, "Unable to connect to Snowflake!!!", None


def execute_query(logger, sf_ctx, query: str):
    cur = sf_ctx.cursor()
    sf_query_id = None
    try:
        logger.info(f"QUERY: {query}")
        cur.execute(query)
    except sf.errors.ProgrammingError as e:
        logger.error(e)
        logger.info(str(traceback.format_exc()))
        logger.error('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return False, "Unable to execute given query!!!", e.sfqid
    else:
        sf_query_id = cur.sfqid
        cur.close()
        return True, "Query executed successfully!!!", sf_query_id


def fetch_pandas_old(logger, sf_ctx, query: str):
    cur = sf_ctx.cursor()
    df = None
    try:
        logger.info(f"QUERY: {query}")
        cur.execute(query)

        rows = 0
        while True:
            dat = cur.fetchmany(50000)
            if not dat:
                break
            df = pd.DataFrame(dat, columns=cur.description)
            rows += df.shape[0]
        logger.info("Total row count: {rows}")

    except sf.errors.ProgrammingError as e:
        logger.error(e)
        logger.info(str(traceback.format_exc()))
        logger.error('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return False, "Unable to execute given query!!!", None
    else:
        cur.close()
        return True, "Query executed successfully!!!", df
