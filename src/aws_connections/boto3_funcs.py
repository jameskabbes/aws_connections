import aws_connections
import boto3

def get_resource( *args, **kwargs ):

    return boto3.resource( *args, **kwargs, **aws_connections.cred_dict )

def get_client( *args, **kwargs ):

    return boto3.client( *args, **kwargs, **aws_connections.cred_dict )

def set_credentials( cred_dict: dict ):

    """set the cred_dict"""
    
    aws_connections.cred_dict = cred_dict

