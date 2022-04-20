from parent_class import ParentClass
import boto3

class Connection( ParentClass ):

    """ can be initialized with Connection( "s3", secret_id = "XXX", secret_pass = "XXX" ) """

    def __init__( self, *args, **kwargs ):

        ParentClass.__init__( self )

        self.resource = self.get_resource( *args, **kwargs )
        self.client = self.get_client( *args, **kwargs )

    def get_resource( self, *args, **kwargs ):
        return boto3.resource( *args, **kwargs )

    def get_client( self, *args, **kwargs ):
        return boto3.client( *args, **kwargs )



