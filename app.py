#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os

import aws_cdk as cdk
import cdk_nag
from aws_cdk import Aspects

from video_bleeping.video_bleeping_stack import VideoBleepingStack


app = cdk.App()
VideoBleepingStack(app, "VideoBleepingStack")

# cdk-nag
cdk_nag.NagSuppressions.add_resource_suppressions(
    app,
    apply_to_children=True,
    suppressions=[
        cdk_nag.NagPackSuppression(
            id="AwsSolutions-IAM4",
            reason="AWS Managed Service Role",
            applies_to=[
                "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            ],
        ),
       cdk_nag.NagPackSuppression(
            id="AwsSolutions-IAM5",
            reason="Read_Write access to specific S3 buckets/ Transcription jobs policy requires wildcard * resource",
            applies_to=[
                "Action::s3:Abort*",
                "Action::s3:DeleteObject*",
                "Action::s3:List*",
                "Action::s3:GetBucket*",
                "Action::s3:GetObject*",
                "Resource::*",
                "Resource::<proxybucketB234A5C4.Arn>/*",
                "Resource::<resourcesbucketEE9A49EF.Arn>/*",
                "Resource::<destinationbucket84C050D8.Arn>/*",
                "Resource::<ingestbucket0022CD63.Arn>/*",
                "Resource::arn:<AWS::Partition>:s3:::cdk-hnb659fds-assets-<AWS::AccountId>-<AWS::Region>/*",
            ],
       ),
       {"id": "AwsSolutions-S1", "reason": "S3 Access logs not required for this demo workflow"},
       {"id": "AwsSolutions-L1", "reason": "Lambda configured to the latest runtime using a parameter"},
       {"id": "AwsSolutions-CFR1", "reason": "Demo CloudFront distribution"},
       {"id": "AwsSolutions-CFR2", "reason": "Demo CloudFront distribution"},
       {"id": "AwsSolutions-CFR3", "reason": "Demo CloudFront distribution"},
       {"id": "AwsSolutions-CFR4", "reason": "Demo CloudFront distribution"},
 
    ],
)

Aspects.of(app).add(
    cdk_nag.AwsSolutionsChecks(log_ignores=True, verbose=True, reports=True)
)

app.synth()
