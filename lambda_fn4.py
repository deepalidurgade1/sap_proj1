# lambda function 4 : - Its triggered by any incoming file in "Enrich zone".
# This starts Crawler4 called "crawler4-sap-enrich-zone" to get catlog table

iimport json
import boto3
from botocore.exceptions import ClientError

AWS_REGION = "ap-south-1"

def lambda_handler(event, context):
    
    session = boto3.session.Session()
    glue_client = session.client('glue', region_name=AWS_REGION)
    
    # TODO implement
    print("Event on enrich bucket occured.............")
    print("Event: ", event)
    
    #1st time start the crawler
    response = glue_client.start_crawler(Name='crawler4-sap-enrich-zone')
    print("the enrich crawler started..............")
    print(response)
    print("------------------------------------------------")
   
