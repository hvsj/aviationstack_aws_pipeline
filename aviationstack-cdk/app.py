#!/usr/bin/env python3

import aws_cdk as cdk

from aviationstack_cdk.aviationstack_cdk_stack import AviationstackCdkStack


app = cdk.App()
AviationstackCdkStack(app, "AviationstackCdkStack")

app.synth()
