import boto3
from aws_credentials import AWS_Creds 

def get_resource( *args, **kwargs ):

    return boto3.resource( *args, **kwargs, **Cred.dict )

def get_client( *args, **kwargs ):

    return boto3.client( *args, **kwargs, **Cred.dict )

def import_credentials( path, role, set_creds = True):

    '''
    path is the filepath to aws_creds.txt
    '''

    Cred = AWS_Creds.import_Cred( role, path = path )

    ### if something besides None was returned
    if AWS_Creds.is_Cred( Cred ):
        if set_creds:
            set_credentials( Cred )

        return Cred.dict

    return {}

def import_blank_credentials( set_creds = True ):
        
    Cred = AWS_Creds.AWS_Cred() # make a blank one, for using with SageMaker or somewhere already authenticated
    
    if set_creds:
        set_credentials( Cred )
    
    return Cred.dict    
    
def set_credentials(Cred_inst):

    '''Set the Cred class as global variables '''
    global Cred
    Cred = Cred_inst
