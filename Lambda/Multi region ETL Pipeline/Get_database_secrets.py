import boto3
import json

def lambda_handler(event, context):
    secret_name = "company/rdsCredentials"
    client = boto3.client("secretsmanager")
    secret_value = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(secret_value["SecretString"])
    return secret
