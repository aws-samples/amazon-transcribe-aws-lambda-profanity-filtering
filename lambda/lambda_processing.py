# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import os

from pydub import AudioSegment


# Environment Variables
PROXY_BUCKET = os.environ['PROXY_BUCKET']
OUTPUT_BUCKET = os.environ['DESTINATION_BUCKET']
RESOURCES_BUCKET = os.environ['RESOURCES_BUCKET']
MEDIACONVERT_EXECUTION_ROLE_ARN = os.environ['MEDIACONVERT_EXECUTION_ROLE_ARN']
    

# AWS services clients
# AWS Elemental MediaConvert: 
mediaconvert_client = boto3.client('mediaconvert')
endpoints = mediaconvert_client.describe_endpoints()
mediaconvert_client = boto3.client(
    'mediaconvert',
    endpoint_url=endpoints['Endpoints'][0]['Url'], 
    verify=False
)

# Amazon S3:
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def handler(event, context):
    #print( json.dumps(event,default=str) )
    
    assetID , emc_job_id = event["detail"]["TranscriptionJobName"].split('___')
    
    try:
        # Get MediaConvert job
        job = mediaconvert_client.get_job(Id=emc_job_id)
        #print( json.dumps(job, default=str) )
        
        # Source Asset
        source_s3_uri = job["Job"]["UserMetadata"]["Source"]

        # Transcribe Output
        transcription_file_key = "transcriptions/" + assetID + "/transcription.json"
        transcription_vtt_file_key = "transcriptions/" + assetID + "/transcription.vtt"
        
        # Audio proxy file key
        s3_audio_proxy_key = "audio_proxy/" + assetID + "/audio.wav"
        
        # Load the transcription results' json file
        json_s3_object = s3.Object(PROXY_BUCKET, transcription_file_key)
        transcription_results_text = json_s3_object.get()['Body'].read().decode('utf-8')
        
        if '***' not in transcription_results_text:
            # No masked words found, simply pass the initial audio source file to MediaConvert  
            print("No Masked words found in the transcription, the original audio will be used")
            s3_audio_redacted_key = s3_audio_proxy_key

        else:
            transcription_results = json.loads( transcription_results_text )
        
            # Load the audio proxy file into pydub AudioSegment
            s3_client.download_file(PROXY_BUCKET, s3_audio_proxy_key, "/tmp/source.wav")
            audio_proxy_wav = AudioSegment.from_wav("/tmp/source.wav")

            # Load the beep audio file from the Resources bucket
            s3_client.download_file(RESOURCES_BUCKET, "Audio/beep.wav", "/tmp/beep.wav")
            beep = AudioSegment.from_wav("/tmp/beep.wav")

            # Parse the transcription results looking for masked "***" words
            # For each masked word, mute/beep the audio using pydub
            previous_end_index = 0
            redacted_audio = AudioSegment.empty()
            
            for item in transcription_results["results"]["items"]:
                if item["type"] == "pronunciation" and item["alternatives"][0]["content"] == "***":
                    s = int(float(item["start_time"]) *1000) #in ms
                    e = int(float(item["end_time"]) *1000)
                    #redacted_audio = redacted_audio + audio_proxy_wav[previous_end_index:s] + AudioSegment.silent(duration=(e-s))
                    redacted_audio = redacted_audio + audio_proxy_wav[previous_end_index:s] + beep[:(e-s)]
                    previous_end_index = e

            # Add the last segement to the redacted audio file, then upload the file to S3  
            redacted_audio = redacted_audio + audio_proxy_wav[previous_end_index:]  
            redacted_audio.export('/tmp/audio_redacted.wav', format="wav")
            s3_audio_redacted_key = "audio_proxy/" + assetID + "/audio_redacted.wav" 
            s3_client.upload_file('/tmp/audio_redacted.wav', PROXY_BUCKET, s3_audio_redacted_key)
        
        
        # Push MediaConvert job to produce the final redacted asset
        emc_destination = 's3://' + OUTPUT_BUCKET + '/' + assetID + '/hls/index'
        
        EMC_JOB_SETTINGS["Inputs"][0]["FileInput"] = source_s3_uri
        EMC_JOB_SETTINGS["Inputs"][0]["AudioSelectors"]["Audio Selector 1"]["ExternalAudioFileInput"] = 's3://' + PROXY_BUCKET + '/' + s3_audio_redacted_key
        EMC_JOB_SETTINGS["Inputs"][0]["CaptionSelectors"]["Captions Selector 1"]["SourceSettings"]["FileSourceSettings"]["SourceFile"] = 's3://' + PROXY_BUCKET + '/' + transcription_vtt_file_key
        EMC_JOB_SETTINGS["OutputGroups"][0]["OutputGroupSettings"]["HlsGroupSettings"]["Destination"] = emc_destination
        
        jobMetadata = {
            "AssetID": assetID,
            "Destination": emc_destination
        }

        # Push the job to MediaConvert service
        job = mediaconvert_client.create_job(Role=MEDIACONVERT_EXECUTION_ROLE_ARN, \
            UserMetadata=jobMetadata, Settings=EMC_JOB_SETTINGS)
        
        job_status = job["Job"]["Status"]
        print( f"MediaConvert job status: {job_status}" )
    
    except Exception as e:
       print("Exception: {}".format(e))
       return 0
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Audio file processing completed and MediaConvert Job Created - Status: {job_status}')
    }


EMC_JOB_SETTINGS = json.loads("""
{
    "TimecodeConfig": {
      "Source": "ZEROBASED"
    },
    "OutputGroups": [
      {
        "Name": "Apple HLS",
        "Outputs": [
          {
            "ContainerSettings": {
              "Container": "M3U8",
              "M3u8Settings": {}
            },
            "VideoDescription": {
              "Width": 960,
              "Height": 540,
              "CodecSettings": {
                "Codec": "H_264",
                "H264Settings": {
                  "FramerateDenominator": 1001,
                  "GopSize": 2,
                  "MaxBitrate": 2000000,
                  "FramerateControl": "SPECIFIED",
                  "RateControlMode": "QVBR",
                  "FramerateNumerator": 30000,
                  "SceneChangeDetect": "TRANSITION_DETECTION",
                  "GopSizeUnits": "SECONDS"
                }
              }
            },
            "AudioDescriptions": [
              {
                "CodecSettings": {
                  "Codec": "AAC",
                  "AacSettings": {
                    "Bitrate": 96000,
                    "CodingMode": "CODING_MODE_2_0",
                    "SampleRate": 48000
                  }
                }
              }
            ],
            "OutputSettings": {
              "HlsSettings": {}
            },
            "NameModifier": "_av"
          },
          {
            "ContainerSettings": {
              "Container": "M3U8",
              "M3u8Settings": {}
            },
            "OutputSettings": {
              "HlsSettings": {}
            },
            "NameModifier": "_vtt",
            "CaptionDescriptions": [
              {
                "CaptionSelectorName": "Captions Selector 1",
                "DestinationSettings": {
                  "DestinationType": "WEBVTT",
                  "WebvttDestinationSettings": {}
                },
                "LanguageCode": "ENG"
              }
            ]
          }
        ],
        "OutputGroupSettings": {
          "Type": "HLS_GROUP_SETTINGS",
          "HlsGroupSettings": {
            "SegmentLength": 6,
            "Destination": "",
            "MinSegmentLength": 0
          }
        }
      }
    ],
    "Inputs": [
      {
        "AudioSelectors": {
          "Audio Selector 1": {
            "DefaultSelection": "DEFAULT",
            "ExternalAudioFileInput": ""
          }
        },
        "VideoSelector": {},
        "TimecodeSource": "ZEROBASED",
        "CaptionSelectors": {
          "Captions Selector 1": {
            "SourceSettings": {
              "SourceType": "WEBVTT",
              "FileSourceSettings": {
                "SourceFile": ""
              }
            }
          }
        },
        "FileInput": ""
      }
    ]
}
""")