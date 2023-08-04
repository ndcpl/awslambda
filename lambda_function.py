from __future__ import print_function

import boto3
from decimal import Decimal
import json
import urllib

rekognition = boto3.client('rekognition')
client = boto3.client('sns')

def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})

    
    return response

def lambda_handler(event, context):
    print (event)
    #Loops through every file uploaded

    for record in event['Records']:

        #pull the body out & json load it

        jsonmaybe=(record["body"])

        jsonmaybe=json.loads(jsonmaybe)

        #now the normal stuff works

        bucket = jsonmaybe["Records"][0]["s3"]["bucket"]["name"]
        print(bucket)

        key=jsonmaybe["Records"][0]["s3"]["object"]["key"]
        print(key)
        
    try:

        # Calls rekognition DetectLabels API to detect labels in S3 object
        response = detect_labels(bucket, key)
        
        tosend=""
        
        for Label in response["Labels"]:
            # print (Label["Name"] + Label["Confidence"])
            
            print ('{0} - {1}%'.format(Label["Name"], Label["Confidence"]))
            tosend+= '{0} - {1}% '.format(Label["Name"], Label["Confidence"])
        
        
        print(response)
        message = client.publish(TargetArn='arn:aws:sns:us-east-1:323708512535:notify_users', Message=tosend ,Subject='Uploaded Image Labels')
        return response
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
