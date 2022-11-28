# lambda function 1 : - Its triggered by any incoming file in "Raw zone".
# This starts Crawler1 called "crawler1-sap-raw-zone" to get catlog table

import json
import boto3
from botocore.exceptions import ClientError


AWS_REGION = "ap-south-1"

def lambda_handler(event, context):
    
    session = boto3.session.Session()
    glue_client = session.client('glue', region_name=AWS_REGION)
    
    # TODO implement
    print("Event on raw bucket occured.............")
    print("Event: ", event)
    
    #1st time start the crawler
    response = glue_client.start_crawler(Name='crawler1-sap-raw-zone')
    print("the crawler started..............")
    print(response)
 
   
   