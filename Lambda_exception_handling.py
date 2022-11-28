# Lambda for exception handling
# If any glue job fails, an e-mail is sent via SNS

import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    
    client = boto3.client('sns')
    snsArn = 'arn:aws:sns:ap-south-1:035336637812:sap-step-function-topic'
   
    message = "The Step function aborted due to following exception"
    error_cause = message    # + "\n Error: " + str(event['Error']) + "\n" + str(json.loads(event['Cause'])['ErrorMessage'])
    # print("Event: ", error_cause)
    
    response = client.publish(
        TopicArn = snsArn,
        Message = error_cause,
        Subject='Notification from step function'
    )