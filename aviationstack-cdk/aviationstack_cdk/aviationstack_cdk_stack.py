from constructs import Construct
from aws_cdk import (
    Duration,
    Stack,
    aws_s3 as _s3,
    aws_lambda as _lambda
)

from util import CURRENT_DIR, get_lambda_layer_arn


class AviationstackCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        src_bkt = _s3.CfnBucket(scope=self, id='AviationStackS3Bucket', bucket_name='aviationstack-bkt')

        api_poller_lambda_code = _lambda.Code.from_asset(path= str(CURRENT_DIR))
        api_poller_envi_vars = _lambda.CfnFunction.EnvironmentProperty(
            variables={
                "env": "dev"
            }
        )
        api_poller_attributes: dict = {
            'id': 'APIPollerFunction',
            'function_name': 'API-Poller',
            'description': '',
            'code': api_poller_lambda_code,
            'handler': _lambda.Runtime.PYTHON_3_10,
            'memory_size': 1792,
            'timeout': Duration.seconds(300),
            'architecture': _lambda.Architecture.X86_64,
            'environment': api_poller_envi_vars

        }
        api_poller_lambda_fun = _lambda.Function(scope=self, **api_poller_attributes)
