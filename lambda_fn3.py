# lambda function 3 : - Its triggered by any incoming file in "confirm zone".
# This starts Crawler3 called "crawler3-sap-confirm-zone" to get catlog table

import json
import boto3
from botocore.exceptions import ClientError


AWS_REGION = "ap-south-1"

def lambda_handler(event, context):
    
    session = boto3.session.Session()
    glue_client = session.client('glue', region_name=AWS_REGION)
    
    # TODO implement
    print("Event on confirm bucket occured.............")
    print("Event: ", event)
    
    #1st time start the crawler
    response = glue_client.start_crawler(Name='crawler3-sap-confirm-zone')
    print("the confirm crawler started..............")
    print(response)
    print("------------------------------------------------")
 
   
   