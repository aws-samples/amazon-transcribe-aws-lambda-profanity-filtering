# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import uuid
import boto3
import os


# Environment Variables
MEDIACONVERT_EXECUTION_ROLE_ARN = os.environ['MEDIACONVERT_EXECUTION_ROLE_ARN']
PROXY_BUCKET = os.environ['PROXY_BUCKET']
WORKLOAD_STAGE = os.environ['WORKLOAD_STAGE']
WORKLOAD_NAME = os.environ['WORKLOAD_NAME']


# AWS services clients
# AWS Elemental MediaConvert: 
mediaconvert_client = boto3.client('mediaconvert')
endpoints = mediaconvert_client.describe_endpoints()
mediaconvert_client = boto3.client(
    'mediaconvert',
    endpoint_url=endpoints['Endpoints'][0]['Url'], 
    verify=False
)


def handler(event, context):
    #print( json.dumps(event,default=str) )

    assetID = str(uuid.uuid4())

    sourceS3Bucket = event['Records'][0]['s3']['bucket']['name']
    sourceS3Key = event['Records'][0]['s3']['object']['key']
    sourceS3URI = 's3://'+ sourceS3Bucket + '/' + sourceS3Key
    
    emcDestination = 's3://' + PROXY_BUCKET + '/audio_proxy/' + assetID + '/audio'
    
    try:
        EMC_JOB_SETTINGS["Inputs"][0]["FileInput"] = sourceS3URI
        EMC_JOB_SETTINGS["OutputGroups"][0]["OutputGroupSettings"]["FileGroupSettings"]["Destination"] = emcDestination
        
        jobMetadata = {
            "AssetID": assetID,
            "Source": sourceS3URI,
            "SourceBucket": sourceS3Bucket,
            "SourceKey": sourceS3Key,
            "Destination": emcDestination,
            "Stage":WORKLOAD_STAGE,
            "Workload":WORKLOAD_NAME
        }

        # Push the job to MediaConvert service
        job = mediaconvert_client.create_job(
            Role=MEDIACONVERT_EXECUTION_ROLE_ARN,
            UserMetadata=jobMetadata, 
            Settings=EMC_JOB_SETTINGS
        )
        
        job_status = job["Job"]["Status"]
        print( f"MediaConvert job status: {job_status}" )

    except Exception as e:
       print("Exception: {}".format(e))
       return 0
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'MediaConvert Job Created - Status: {job_status}')
    }


EMC_JOB_SETTINGS = json.loads("""
  {
    "TimecodeConfig": {
      "Source": "ZEROBASED"
    },
    "OutputGroups": [
      {
        "Name": "File Group",
        "Outputs": [
          {
            "ContainerSettings": {
              "Container": "RAW"
            },
            "AudioDescriptions": [
              {
                "AudioSourceName": "Audio Selector 1",
                "CodecSettings": {
                  "Codec": "WAV",
                  "WavSettings": {
                    "Channels": 2
                  }
                }
              }
            ],
            "Extension": "wav"
          }
        ],
        "OutputGroupSettings": {
          "Type": "FILE_GROUP_SETTINGS",
          "FileGroupSettings": {
            "Destination": ""
          }
        }
      }
    ],
    "Inputs": [
      {
        "AudioSelectors": {
          "Audio Selector 1": {
            "DefaultSelection": "DEFAULT"
          }
        },
        "VideoSelector": {},
        "TimecodeSource": "ZEROBASED",
        "FileInput": ""
      }
    ]
  }
""")