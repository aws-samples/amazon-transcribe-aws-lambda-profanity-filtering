# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk as cdk

from aws_cdk import (
    Duration,
    Stack,
    Size,
    aws_lambda as _lambda,
    aws_lambda_event_sources as eventsources,
    aws_s3 as s3,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3_deployment as s3deploy,
    RemovalPolicy,
)

from constructs import Construct


class VideoBleepingStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Parameters and configs
        lambda_runtime = _lambda.Runtime.PYTHON_3_11
        
        ingest_bucket_removal_policy = RemovalPolicy.RETAIN
        ingest_bucket_auto_delete_objects=False

        proxy_bucket_removal_policy = RemovalPolicy.DESTROY
        proxy_bucket_auto_delete_objects=True

        destination_bucket_removal_policy = RemovalPolicy.RETAIN
        destination_bucket_auto_delete_objects=False

        resources_bucket_removal_policy = RemovalPolicy.DESTROY
        resources_bucket_auto_delete_objects=True

        deploy_demo_cloudfront_distribution=True

        # Used to filter EventBridge events
        workload_name = "VideoBleeping"
        workload_ingest_stage_name = "INGEST"


        # Create required S3 buckets
        ingest_bucket = s3.Bucket(
            self, "ingest_bucket",
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=ingest_bucket_removal_policy,
            auto_delete_objects=ingest_bucket_auto_delete_objects
        )

        ingest_bucket.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=Duration.days(3),
            enabled=True,
        )

        proxy_bucket = s3.Bucket(
            self, 'proxy_bucket',
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=proxy_bucket_removal_policy,
            auto_delete_objects=proxy_bucket_auto_delete_objects,
        )

        proxy_bucket.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=Duration.days(3),
            enabled=True,
            expiration=Duration.days(30),
        )

        destination_bucket = s3.Bucket(
            self, "destination_bucket",
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=destination_bucket_removal_policy,
            auto_delete_objects=destination_bucket_auto_delete_objects
        )

        destination_bucket.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=Duration.days(3),
            enabled=True,
        )

        resources_bucket = s3.Bucket(
            self, 'resources_bucket',
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=resources_bucket_removal_policy,
            auto_delete_objects=resources_bucket_auto_delete_objects,
            enforce_ssl=True,
        )


        # Upload all resources (config and audio beep file) to S3 resources_bucket

        s3deploy.BucketDeployment(
            self, 'upload_resources',
            sources=[s3deploy.Source.asset('./resources')],
            destination_bucket=resources_bucket,
        )


        # Ingest Lambda function and MediaConvert Execution Role

        emc_role = iam.Role(
            self, 'mediaconvert_audio_proxy_role',
            assumed_by=iam.ServicePrincipal('mediaconvert.amazonaws.com'),
            description='Execution Role passed to MediaConvert job that creates the wav audio proxy file',
        )

        ingest_bucket.grant_read(emc_role)
        proxy_bucket.grant_read_write(emc_role)
        
        ingest_function = _lambda.Function(
            self, 'media_ingest_function',
            runtime=lambda_runtime,
            code=_lambda.Code.from_asset('lambda'),
            handler='media_ingest.handler',
            environment={
                'PROXY_BUCKET': proxy_bucket.bucket_name,
                'MEDIACONVERT_EXECUTION_ROLE_ARN': emc_role.role_arn,
                'WORKLOAD_NAME': workload_name,
                'WORKLOAD_STAGE': workload_ingest_stage_name,
            },
            timeout=Duration.seconds(30),
        )

        emc_role.grant_pass_role(ingest_function)
        
        ingest_function.add_to_role_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "mediaconvert:DescribeEndpoints",
                    "mediaconvert:CreateJob"
                ],
                "Resource": "*"
            })
        )

        ingest_function.add_event_source(
            eventsources.S3EventSource(
                ingest_bucket,
                events=[s3.EventType.OBJECT_CREATED],
                #filters=[s3.NotificationKeyFilter(prefix="subdir/")]
            )
        )
        
        # Transcribe Lambda, Event Bridge Rule and permissions

        transcribe_role = iam.Role(
            self, 'transcribe_access_role',
            assumed_by=iam.ServicePrincipal('transcribe.amazonaws.com'),
            description='Role to allow Transcribe access to S3 bucket',
        )

        proxy_bucket.grant_read_write(transcribe_role)

        transcribe_function = _lambda.Function(
            self, 'transcription_function',
            runtime=lambda_runtime,
            code=_lambda.Code.from_asset('lambda'),
            handler='transcription.handler',
            environment={
                'PROXY_BUCKET': proxy_bucket.bucket_name,
                'RESOURCES_BUCKET': resources_bucket.bucket_name,
                'TRANSCRIBE_ACCESS_ROLE_ARN':transcribe_role.role_arn,
            },
            timeout=Duration.seconds(30),
        )

        transcribe_role.grant_pass_role(transcribe_function)

        resources_bucket.grant_read(transcribe_function)
        
        transcribe_function.add_to_role_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "transcribe:StartTranscriptionJob",
                ],
                "Resource": "*"
            })
        )
        
        emc_job_completed_rule = events.Rule(
            self, "Mediaconvert_Job_Completed_Rule",
            event_pattern= events.EventPattern(
                source=["aws.mediaconvert"],
                detail_type=["MediaConvert Job State Change"],
                detail= {
                    "status" : ["COMPLETE"],
                    "userMetadata" : {
                        "Stage" : [workload_ingest_stage_name],
                        "Workload" : [workload_name]
                    }
                }
            ),
            targets=[targets.LambdaFunction(transcribe_function)]
        )

        
        # Audio Processing Lambda

        pydub_layer = _lambda.LayerVersion(self, "pydub_layer",
            code=_lambda.Code.from_asset('layer_pydub'),
            compatible_architectures=[_lambda.Architecture.X86_64],
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9, _lambda.Runtime.PYTHON_3_10, _lambda.Runtime.PYTHON_3_11]                                   
        )

        processing_emc_role = iam.Role(
            self, 'mediaconvert_video_processing_role',
            assumed_by=iam.ServicePrincipal('mediaconvert.amazonaws.com'),
            description='Execution Role passed to MediaConvert job that transcodes the video file',
        )

        ingest_bucket.grant_read(processing_emc_role)
        destination_bucket.grant_read_write(processing_emc_role)
        proxy_bucket.grant_read(processing_emc_role)
        
        processing_function = _lambda.Function(
            self, 'processing_function',
            runtime=lambda_runtime,
            code=_lambda.Code.from_asset('lambda'),
            handler='lambda_processing.handler',
            environment={
                'PROXY_BUCKET': proxy_bucket.bucket_name,
                'DESTINATION_BUCKET': destination_bucket.bucket_name,
                'RESOURCES_BUCKET': resources_bucket.bucket_name,
                'MEDIACONVERT_EXECUTION_ROLE_ARN':processing_emc_role.role_arn,
            },
            timeout=Duration.seconds(300),
            layers=[pydub_layer],
            memory_size=4096,
            ephemeral_storage_size=Size.mebibytes(4096),
        )

        processing_emc_role.grant_pass_role(processing_function)
        proxy_bucket.grant_read_write(processing_function)
        resources_bucket.grant_read(processing_function)

        processing_function.add_to_role_policy(
            iam.PolicyStatement.from_json({
                "Effect": "Allow",
                "Action": [
                    "mediaconvert:DescribeEndpoints",
                    "mediaconvert:CreateJob",
                    "mediaconvert:GetJob"
                ],
                "Resource": "*"
            })
        )

        transcribe_job_completed_rule = events.Rule(
            self, "Transcribe_Job_Completed_Rule",
            event_pattern= events.EventPattern(
                source=["aws.transcribe"],
                detail_type=["Transcribe Job State Change"],
                detail= {"TranscriptionJobStatus": ["COMPLETED"]}
            ),
            targets=[targets.LambdaFunction(processing_function)]
        )

        
        # CloudFront distribution to playback generated HLS asset
        if deploy_demo_cloudfront_distribution:
            cf_dist = cloudfront.Distribution(
                self, 'demo_distribution',
                default_behavior=cloudfront.BehaviorOptions(
                    origin=origins.S3Origin(destination_bucket),
                    response_headers_policy=cloudfront.ResponseHeadersPolicy.CORS_ALLOW_ALL_ORIGINS,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                ),
            )


        # Outputs
        cdk.CfnOutput(
            self, "S3_Ingest_Bucket_Name",
            export_name="IngestBucketName",
            value=ingest_bucket.bucket_name
        )
        
        if deploy_demo_cloudfront_distribution:
            cdk.CfnOutput(
                self, "CloudFront_Dirtribution_Domain_Name",
                export_name="CloudFrontDirtributionDomainName",
                value=cf_dist.domain_name
            )