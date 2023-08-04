import json
import boto3

s3_client = boto3.client('s3')
rekognition_client = boto3.client('rekognition')
sns_client = boto3.client('sns')
sqs_queue_url = 'https://sqs.us-east-1.amazonaws.com/323708512535/SQSImage'
sns_topic_arn = 'arn:aws:sns:us-east-1:323708512535:notify_users'

def lambda_handler(event, context):
    
  # Print the event to check its structure
    
    for record in event['Records']:
        record= json.loads(record['body'])
        # Access the S3 bucket and object key from the record
        s3_bucket = record['Records'][0]['s3']['bucket']['name']
        s3_object_key = record['Records'][0]['s3']['object']['key']
        try:    
            # Perform image analysis using AWS Rekognition
            response = rekognition_client.detect_labels(
                Image={
                    'S3Object': {
                        'Bucket': s3_bucket,
                        'Name': s3_object_key
                    }
                },
                MaxLabels=10,
                MinConfidence=80
            )
            
            # Determine if the analysis is successful based on desired labels and confidence threshold
            desired_labels = ['Widgets','Smartphones']  # Add your desired labels here
            confidence_threshold = 80  # Set your desired confidence threshold
            
            labels = [label['Name'] for label in response['Labels']]
            confidences = [label['Confidence'] for label in response['Labels']]
            
            success = any(label in desired_labels and confidence >= confidence_threshold
                          for label, confidence in zip(labels, confidences))
            
            print(f"Labels: {labels}")
            print(f"Confidences: {confidences}")
            print(f"Success: {success}")
            
            # Move the original image to the appropriate folder in the S3 bucket
            destination_folder = 'analyzed/success' if success else 'analyzed/failure'
            destination_key = f'{destination_folder}/{s3_object_key}'
            
            s3_client.copy_object(
                Bucket=s3_bucket,
                CopySource={'Bucket': s3_bucket, 'Key': s3_object_key},
                Key=destination_key
            )
            
            print(f"Image moved to: s3://{s3_bucket}/{destination_key}")
            
            # Delete the original image from the /images folder
            s3_client.delete_object(
                Bucket=s3_bucket,
                Key=s3_object_key
            )
            
            print(f"Image deleted: s3://{s3_bucket}/{s3_object_key}")
            
            # Publish the analysis results to the SNS topic
            message = {
                'success': success,
                'labels': labels,
                'confidences': confidences
            }
            
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=json.dumps(message)
            )
            
            print("Analysis results published to SNS")
        
        except Exception as e:
            print(f"Error processing object: {s3_object_key} from bucket: {s3_bucket}")
            print(str(e))
            raise e
    
    return {
        'statusCode': 200,
        'body': json.dumps('Image processing completed.')
    }
