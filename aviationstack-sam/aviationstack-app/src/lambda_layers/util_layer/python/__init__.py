# ----- In-built modules
import sys
import json
import time
import base64
import logging
import traceback
import functools
from datetime import datetime

# ----- user-defined modules
from aws_util import upload_file_like_obj_to_s3
from log_util import get_timestamp
from snowflake_util import execute_query

# ----- 3rd-party modules
import requests
import pandas as pd

import snowflake.connector as sf

import boto3
from botocore.exceptions import ClientError



