# in-built modules
from pathlib import Path

#   3rd-party modules
import boto3
from botocore.exceptions import ClientError

CURRENT_DIR = Path(__file__).parent if "__file__" in locals() else Path.cwd()


def get_lambda_layer_arn(layer_output_keys: list, stack_name: str, **kwargs):

    try:
        if 'profile_name' in kwargs:
            my_session = boto3.session.Session(profile_name=kwargs['profile_name'])
            cf_client = my_session.client('cloudformation')
        else:
            cf_client = boto3.client('cloudformation')

        response = cf_client.describe_stacks(StackName=stack_name)
        outputs = response['Stacks'][0]['Outputs']

        layer_arns = {}

        for output in outputs:
            if output['OutputKey'] in layer_output_keys:
                layer_arns[output['OutputKey']] = output['OutputValue']

        if layer_arns:
            if len(layer_output_keys) == len(layer_arns):
                return True, 'All layer output keys found in given STACK: {} outputs', layer_arns
            else:
                return True, 'One or more layer output keys not found in given STACK: {} outputs', layer_arns
        else:
            return False, 'No layer output keys found in given STACK: {} outputs', None
    except ClientError as e:
        print(e)
        return False, "Exception Occurred while fetching lambda layer arn's!!", None
