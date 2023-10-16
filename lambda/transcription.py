# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import os


# Environment Variables
PROXY_BUCKET = os.environ['PROXY_BUCKET']
RESOURCES_BUCKET = os.environ['RESOURCES_BUCKET']
TRANSCRIBE_ACCESS_ROLE_ARN = os.environ['TRANSCRIBE_ACCESS_ROLE_ARN']
    

# AWS services clients 
# Amazon Transcribe:
transcribe_client = boto3.client('transcribe')

# Amazon S3:
s3 = boto3.resource('s3')


def handler(event, context):
    #print( json.dumps(event) )

    
    try:
        # Load the language configs from S3
        config_s3_object = s3.Object(RESOURCES_BUCKET, 'Config/config.json')
        config = json.loads( config_s3_object.get()['Body'].read().decode('utf-8') )

        # By default, detect English US language without filtering
        language_settings = {
            "LanguageCode" : "en-US",
            "Settings":{
                "VocabularyFilterMethod":"mask"
            }
        }

        if 'Transcribe Language Codes' in config: 
            if len(config['Transcribe Language Codes']) == 1:
                # One language code is provided 
                lc = config['Transcribe Language Codes'][0]
                language_settings['LanguageCode'] = lc
                if 'Transcribe Language Settings' in config and lc in config['Transcribe Language Settings']:
                    # Vocabulary filter is provided
                    language_settings['Settings']['VocabularyFilterName'] = config['Transcribe Language Settings'][lc]['VocabularyFilterName']
            if len(config['Transcribe Language Codes']) > 1:
                # 2 or more language codes provided, activate auto language detection in Transcribe
                language_settings.pop('LanguageCode')
                language_settings["IdentifyLanguage"] = True
                language_settings["LanguageOptions"]= config['Transcribe Language Codes']
                if 'Transcribe Language Settings' in config:
                    language_settings["LanguageIdSettings"] = {}
                    for lc in config['Transcribe Language Settings']:
                        language_settings["LanguageIdSettings"][lc] = config['Transcribe Language Settings'][lc]
                
        
        proxyURI = event["detail"]["outputGroupDetails"][0]["outputDetails"][0]["outputFilePaths"][0] 
        print("Audio proxy file: {}".format(proxyURI))
        
        assetId = event["detail"]["userMetadata"]["AssetID"]
        transcription_s3_key = "transcriptions/" + assetId + "/transcription.json"
        emc_job_id = event["detail"]["jobId"]
        
        job = transcribe_client.start_transcription_job(
            TranscriptionJobName= assetId + "___" + emc_job_id,
            #MediaSampleRateHertz= 44100,
            MediaFormat= "wav",
            Media= {
                "MediaFileUri": proxyURI
            },
            OutputBucketName= PROXY_BUCKET,
            OutputKey= transcription_s3_key,
            JobExecutionSettings={
                'AllowDeferredExecution': True,
                'DataAccessRoleArn': TRANSCRIBE_ACCESS_ROLE_ARN
            },
            Subtitles={
                'Formats': [
                    'vtt'
                ]
            },
            **language_settings
        )

        transcription_job_status = job["TranscriptionJob"]["TranscriptionJobStatus"]
        print(f"Transcription job status: {transcription_job_status}")
        
    except Exception as e:
       print("Exception: {}".format(e))
       return 0
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Transcribe job created - Status: {transcription_job_status}')
    }
