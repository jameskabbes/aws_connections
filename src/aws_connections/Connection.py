from parent_class import ParentClass
import boto3

class Connection( ParentClass ):

    """ can be initialized with Connection( "s3", secret_id = "XXX", secret_pass = "XXX" ) """

    def __init__( self ):

        ParentClass.__init__( self )

        self.resource = self.get_resource()
        self.client = self.get_client()


    def get_resource( self ):
        
        args = self.cfg['connection.args']
        kwargs = self.cfg['connection.kwargs'].get_ref_dict()
        return boto3.resource( *args, **kwargs )

    def get_client( self, *args, **kwargs ):

        args = self.cfg['connection.args']
        kwargs = self.cfg['connection.kwargs'].get_ref_dict()
        return boto3.client( *args, **kwargs )



