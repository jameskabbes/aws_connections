from __future__ import annotations
import dir_ops as do
import py_starter as ps
import aws_connections
from aws_connections import s3

from typing import Tuple, List

def set_connection( **kwargs ):

    DEFAULT_KWARGS = aws_connections.cred_dict
    joined_kwargs = ps.merge_dicts( DEFAULT_KWARGS, kwargs )

    s3.conn = aws_connections.Connection( 's3', **joined_kwargs )


class S3Dir( do.RemoteDir ):

    STATIC_METHOD_SUFFIX = '_dir'
    INSTANCE_METHOD_ATTS = ['bucket','path','conn']

    DEFAULT_KWARGS = {
        'uri': None,
        'bucket': None,
        'Path': None,   # is synonomous with an S3 Key
        'path': None,
        'conn': None,
    }

    URI_PREFIX = 's3://'

    def __init__( self, *args, **kwargs):

        joined_atts = ps.merge_dicts( S3Dir.DEFAULT_KWARGS, kwargs )
        self.set_atts( joined_atts )

        # set the connection
        if self.conn == None:
            self.conn = aws_connections.s3.conn

        # if a uri is found, this takes precedent
        if self.uri != None:
            self.bucket, self.Path = S3Dir.split_uri( self.uri )

        else:
            
            # user must specify a bucket
            if self.bucket == None:
                print ('No bucket was specified')
                assert False

            # first priority is a Do.Path object            
            if self.Path == None:

                # second priority is a path str
                if self.path == None:
                    print ('No path was specified')
                    assert False
                else:
                    self.Path = do.Dir( self.path )

            self.uri = S3Dir.join_uri( self.bucket, self.Path.p )

        do.RemoteDir.__init__( self, self.path )

        self.inherited_kwargs = { 'bucket': self.bucket, 'conn': self.conn }
        self.DIR_CLASS = S3Dir
        self.PATH_CLASS = S3Path
        self.DIRS_CLASS = S3Dirs
        self.PATHS_CLASS = S3Paths

    def __eq__( self, other_S3Dir ):

        if isinstance( other_S3Dir, S3Dir ):
            return self.uri == other_S3Dir.uri
        return False

    def print_imp_atts( self, **kwargs ):

        return self._print_imp_atts_helper( atts = ['uri'], **kwargs )

    def print_one_line_atts(self, **kwargs ):

        return self._print_one_line_atts_helper( atts = ['type','uri'], **kwargs )

    @staticmethod
    def split_uri( uri: str ) -> Tuple[ str, str ]:
        
        """returns bucket and path
        uri looks like: 's3://bucketname/path/to/file"""

        if uri.startswith( S3Dir.URI_PREFIX ):
            trimmed_uri = uri[ len(S3Dir.URI_PREFIX) : ]
            dirs = trimmed_uri.split( '/' )

            bucket = dirs[0]
            path = '/'.join( dirs[1:] )

            return bucket, path

    @staticmethod
    def join_uri( bucket: str, path: str ) -> str:

        """Given a bucket and a path, generate the S3 uri """

        uri = S3Dir.URI_PREFIX + bucket + '/' + path
        return uri

    @staticmethod
    def create_dir( *args, **kwargs ):
        # It is not possible to create a dir on S3, but we don't want to throw an exception when it doesn't work
        assert True

    @staticmethod
    def exists_dir( bucket: str, path: str, conn: aws_connections.Connection, **kwargs ):
        
        return 'CommonPrefixes' in conn.client.list_objects_v2( Bucket = bucket, Prefix = path, Delimiter = '/', MaxKeys=1 )

    @staticmethod
    def get_size_dir( bucket: str, path: str, conn: aws_connections.Connection,
                    *args, **kwargs ):

        self = S3Dir( bucket = bucket, path = path, conn = conn )
        Paths_inst = self.list_contents_Paths( block_dirs=True, block_paths=False )

        bytes = 0
        for Path_inst in Paths_inst:
            Path_inst.get_size( **kwargs )
            bytes += Path_inst.size 
        
        return bytes

    @staticmethod
    def remove_dir( bucket: str, path: str, conn: aws_connections.Connection, *args, **kwargs ) -> bool:
        conn.resource.Bucket( bucket ).objects.filter( Prefix = path ).delete()

    @staticmethod
    def copy_dir( bucket: str, path: str, conn: aws_connections.Connection, *args, 
                    destination: str = '', destination_bucket = None, **kwargs ):

        remote_Dir = S3Dir( bucket = bucket, path = path )
        remote_Paths = remote_Dir.walk_contents_Paths( block_dirs=True )
        
        valid = True
        for remote_Path in remote_Paths:
            rel_Path = remote_Path.get_rel( remote_Dir )
            destination_path = do.join( destination, rel_Path.path )
            if not remote_Path.copy( *args, destination = destination_path, **kwargs ):
                valid = False
            
        assert valid


    @staticmethod
    def upload_dir( bucket: str, path: str, conn: aws_connections.Connection, *args,
                        destination: str = '', **kwargs ):
        
        local_Dir = do.Dir( destination )
        local_Paths = local_Dir.walk_contents_Paths( block_dirs=True )

        valid = True
        for local_Path in local_Paths:
            rel_Path = local_Path.get_rel( local_Dir )
            remote_Path = S3Path( bucket = bucket, path = do.join( path, rel_Path.path ) )
            if not remote_Path.upload( *args, Destination = local_Path, **kwargs ):
                valid = False

        assert valid

    @staticmethod
    def download_dir( bucket: str, path: str, conn: aws_connections.Connection, *args,
                        destination: str = '', **kwargs ):

        remote_Dir = S3Dir( bucket = bucket, path = path, conn = conn )
        remote_Paths = remote_Dir.walk_contents_Paths( block_dirs=True )
        local_Dir = do.Dir( destination )
        
        valid = True
        for remote_Path in remote_Paths:
            rel_Path = remote_Path.get_rel( remote_Dir )
            local_Path = do.Path( local_Dir.join( rel_Path.path ) )
            if not remote_Path.download( *args, Destination = local_Path, **kwargs ):
                valid = False

        assert valid
    
    @staticmethod
    def list_subfolders_dir(bucket: str, path: str, conn: aws_connections.Connection,
                            print_off: bool = False, **kwargs ) -> List[ str ]:

        prefix = path
        if prefix != '':
            prefix += '/'

        result = conn.client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/', **kwargs )

        subfolders = []
        if 'CommonPrefixes' in result:
            for common_prefix in result[ 'CommonPrefixes' ]:
                full_dir = common_prefix[ 'Prefix' ]
                subfolders.append( S3Dir.get_rel_dir( full_dir, prefix ) )

        if print_off:
            ps.print_for_loop( subfolders )

        return subfolders

    @staticmethod
    def list_files_dir( bucket: str, path: str, conn: aws_connections.Connection, 
                     print_off: bool = False, **kwargs ):

        self = S3Dir( bucket = bucket, path = path, conn = conn )

        prefix = self.Path.path
        if prefix != '':
            prefix += '/'

        result = self.conn.client.list_objects_v2(Bucket = self.bucket, Prefix = prefix, Delimiter = '/', **kwargs)

        filenames = []
        if 'Contents' in result:
            for key in result['Contents']:
                S3Path_inst = S3Path( bucket = bucket, path = key['Key'], conn = conn ) #full Path
                filenames.append( S3Path_inst.get_rel( self ).path )        

        if print_off:
            ps.print_for_loop( filenames )

        return filenames


class S3Path( S3Dir, do.RemotePath ):

    STATIC_METHOD_SUFFIX = '_path'
    INSTANCE_METHOD_ATTS = ['bucket','path','conn']

    def __init__( self, *args, **kwargs ) :

        S3Dir.__init__( self, *args, **kwargs )
        do.RemotePath.__init__( self, self.path )
        self.DIR_CLASS = S3Dir
        self.PATH_CLASS = S3Path
        self.DIRS_CLASS = S3Dirs
        self.PATHS_CLASS = S3Paths

    def print_imp_atts(self, **kwargs):

        return self._print_imp_atts_helper( atts = ['uri','dirs','ending','size'], **kwargs )

    @staticmethod
    def exists_path(bucket: str, path: str, conn: aws_connections.Connection, *args, **kwargs):
        conn.resource.Object( bucket, path ).load()

    @staticmethod
    def upload_path( bucket: str, path: str, conn: aws_connections.Connection, *args,
                        destination: str = '', **kwargs):

        conn.resource.meta.client.upload_file( destination, bucket, path)

    @staticmethod
    def download_path( bucket: str, path: str, conn: aws_connections.Connection, *args,
                        destination: str = '', **kwargs ):

        if destination == '':
            destination = aws_connections._cwd_Dir.join( do.Path.get_filename( path ) )

        conn.resource.meta.client.download_file(bucket, path, destination)

    @staticmethod
    def remove_path( bucket: str, path: str, conn: aws_connections.Connection, *args, **kwargs ):
        conn.client.delete_object( Bucket = bucket, Key = path )

    @staticmethod
    def get_size_path( bucket: str, path: str, conn: aws_connections.Connection,
                        **kwargs ) -> float:

        response = conn.client.head_object(Bucket = bucket, Key = path)
        bytes = response['ContentLength']
        return bytes

    @staticmethod
    def write_path( bucket: str, path: str, conn: aws_connections.Connection, *args, **kwargs):

        """write to a local file, then upload to S3"""

        self = S3Path( bucket = bucket, path = path, conn = conn )
        temp_Path = do.Path( 'TEMP' )

        if not temp_Path.write( **kwargs ):
            assert False
        
        if not self.upload( Destination = temp_Path, **kwargs ):
            assert False

        if not temp_Path.remove( **kwargs ):
            assert False
    
    @staticmethod
    def create_path( bucket: str, path: str, conn: aws_connections.Connection, *args, string = '', **kwargs ):

        self = S3Path( bucket = bucket, path = path, conn = conn )
        if not self.write( string = string, **kwargs ):
            assert False

    @staticmethod
    def read_path( bucket: str, path: str, conn: aws_connections.Connection, *args, **kwargs ):

        """download the s3 file to a local path and read the contents"""

        temp_Path = do.Path( 'TEMP' )

        self = S3Path( bucket = bucket, path = path, conn = conn )
        self.download( Destination = temp_Path, **kwargs )
            
        contents = temp_Path.read( **kwargs )
        temp_Path.remove( override = True, **kwargs )
                
        return contents

    @staticmethod
    def copy_path( bucket: str, path: str, conn: aws_connections.Connection, *args,
                    destination: str = '', destination_bucket = None, **kwargs ):

        copy_source = {
            'Bucket': bucket,
            'Key': path
        }

        # default to using the same bucket as the current place
        if destination_bucket == None:
            destination_bucket = bucket

        # perform the copy
        conn.resource.meta.client.copy( copy_source, destination_bucket, destination )

    @staticmethod
    def rename_path( bucket: str, path: str, conn: aws_connections.Connection, *args,
                    destination: str = '', destination_bucket = None, **kwargs ):

        self = S3Path( bucket = bucket, path = path, conn = conn )
        if self.copy( destination = destination, destination_bucket = destination_bucket, **kwargs ):
            self.remove( **kwargs )


class S3Dirs( do.RemoteDirs ):

    def __init__( self, *args, **kwargs ):

        do.RemoteDirs.__init__( self, *args, **kwargs )
        self.DIR_CLASS = S3Dir
        self.PATH_CLASS = S3Path
        self.DIRS_CLASS = S3Dirs
        self.PATHS_CLASS = S3Paths


class S3Paths( S3Dirs, do.RemotePaths ):

    def __init__( self, *args, **kwargs ):

        S3Dirs.__init__( self )
        do.RemotePaths.__init__( self, *args, **kwargs )
        self.DIR_CLASS = S3Dir
        self.PATH_CLASS = S3Path
        self.DIRS_CLASS = S3Dirs
        self.PATHS_CLASS = S3Paths



