import kabbes_client
import kabbes_config
import aws_connections

class Client( aws_connections.Connection ):

    _BASE_DICT = {}

    def __init__( self, dict={}, connection_kwargs={} ):

        d = {}
        d.update( Client._BASE_DICT )
        d.update( dict )

        self.Package = kabbes_client.Package( aws_connections._Dir, dict=d )
        self.cfg = self.Package.cfg

        connection_kwargs_node = self.cfg.get_node( 'connection.kwargs', make=True )
        connection_kwargs_node.merge( kabbes_config.Config( dict=connection_kwargs ) )

        aws_connections.Connection.__init__( self )
