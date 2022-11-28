# lambda function 2 : - Its triggered by any incoming file in "transform zone".
# This starts Crawler2 called "crawler2-sap-transform-zone" to get catlog table

import json
import boto3
from botocore.exceptions import ClientError


AWS_REGION = "ap-south-1"

def lambda_handler(event, context):
    
    session = boto3.session.Session()
    glue_client = session.client('glue', region_name=AWS_REGION)
    
    # TODO implement
    print("Event on transform bucket occured.............")
    print("Event: ", event)
    
    #1st time start the crawler
    response = glue_client.start_crawler(Name='crawler2-sap-transform-zone')
    print("the  transform crawler started..............")
    print(response)
    print("------------------------------------------------")
   
   