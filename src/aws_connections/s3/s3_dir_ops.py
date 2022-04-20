from __future__ import annotations
import dir_ops as do
from dir_ops.dir_ops import instance_method
import py_starter as ps
import aws_connections
from aws_connections import s3

import functools
from typing import Tuple, List

def set_connection( **kwargs ):

    DEFAULT_KWARGS = aws_connections.cred_dict
    joined_kwargs = ps.merge_dicts( DEFAULT_KWARGS, kwargs )

    s3.conn = aws_connections.Connection( 's3', **joined_kwargs )

def s3instance_method(method):

    """instance methods call the corresponding staticmethod 
    Example: Dir_instance.exists(*,**) calls Dir.exists_dir( Dir_instance.path,*,** )   """

    @functools.wraps(method)
    def wrapper( self, *called_args, **called_kwargs):

        new_method_name = method.__name__ + self.STATIC_METHOD_SUFFIX
        return self.get_attr( new_method_name )( self.bucket, self.path, self.conn, *called_args, **called_kwargs )
        
    return wrapper


class S3Dir( do.Dir ):

    STATIC_METHOD_SUFFIX = '_dir'

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

        do.Dir.__init__( self, self.path )
        self.PATHS_CLASS = S3Paths
        self.DIR_CLASS = S3Dir

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

    @s3instance_method
    def list_subfolders( self, *args, **kwargs ):
        pass

    @staticmethod
    def list_subfolders_dir(bucket: str, path: str, conn: aws_connections.Connection,
                            print_off: bool = False ) -> List[ str ]:

        prefix = path
        if prefix != '':
            prefix += '/'

        result = conn.client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    
        subfolders = []
        try:
            for i in result.get('CommonPrefixes'):
                subfolders.append(i.get('Prefix'))
                
        except:
            pass

        if print_off:
            ps.print_for_loop( subfolders )

        return subfolders

    def list_files( self, *args, print_off: bool = False, remove_root: bool = True, remove_lower_subfolders: bool = True, **kwargs ):

        prefix = self.Path.path
        if prefix != '':
            prefix += '/'

        response = self.conn.client.list_objects_v2(Bucket = self.bucket, Prefix = prefix, Delimiter = '/')

        filenames = []

        # 1. Add all files immediately underneath
        try:
            for file_dict in response['Contents']:
                filenames.append(file_dict['Key'])
        except:
            print ('S3 Location ' + str(self.uri) + ' does not exist')

        # Note: this will be incredibly inefficient for "walking". Need to redesign this structure for a more customized S3 approach
        if remove_lower_subfolders:
            for i in range(len(filenames)-1, -1, -1):

                # if the filename is from a lower subfolder, remove it
                if len(self.get_rel( do.Dir( filenames[i] ) ).dirs) > 1:
                    del filenames[i]

        # list_objects_v2() also lists the root directory as a path
        if prefix in filenames and remove_root:
            del filenames[ filenames.index( prefix ) ]

        if print_off:
            ps.print_for_loop( filenames )

        return filenames

    def list_contents( self, print_off: bool = True ) -> List[ str ]:

        filenames = []
        filenames.extend( self.list_subfolders() )
        filenames.extend( self.list_files() )

        if print_off:
            ps.print_for_loop( filenames )

        return filenames

    def list_contents_Paths( self, block_dirs: bool = True, block_paths: bool = False ) -> S3Paths:

        Paths_inst = self.PATHS_CLASS()

        # 1. Add all files
        if not block_paths:
            paths = self.list_files()

            for path in paths:
                Paths_inst._add( S3Path( bucket = self.bucket, path = path ) )

        # 2. Add all dirs
        if not block_dirs:
            dirs = self.list_subfolders()

            for dir in dirs:
                Paths_inst._add( S3Dir( bucket = self.bucket, path = dir ) )

           
        return Paths_inst



class S3Path( S3Dir, do.Path ):

    STATIC_METHOD_SUFFIX = '_path'

    def __init__( self, *args, **kwargs ) :

        S3Dir.__init__( self, *args, **kwargs )
        do.Path.__init__( self, self.path )
        self.PATHS_CLASS = S3Paths
        self.DIR_CLASS = S3Dir

    @s3instance_method
    def upload( self, *args, **kwargs ):
        pass

    @staticmethod
    def upload_path( bucket: str, path: str, conn: aws_connections.Connection,
                        local_Path: do.Path = None, local_path: str = None ):

        if do.Path.is_Path( local_Path ):
            local_path = local_Path.path

        conn.resource.meta.client.upload_file(local_path, bucket, path)

    @s3instance_method
    def download( self, *args, **kwargs ):
        pass

    @staticmethod
    def download_path( bucket: str, path: str, conn: aws_connections.Connection,
                        local_Path: do.Path = None, local_path: str = None ):

        if do.Path.is_Path( local_Path ):
            local_path = local_Path.path

        conn.resource.meta.client.download_file(bucket, path, local_path)

    @s3instance_method
    def remove( self, *args, **kwargs ):
        pass

    @staticmethod
    def remove_path( bucket: str, path: str, conn: aws_connections.Connection,
                        override: bool = False, print_off: bool = True ) -> bool:

        """deletes file at path: BE CAREFUL"""

        inp = 'delete'
        if not override:
            inp = input('Type "delete" to delete ' + str(path) + ': ')

        if inp == 'delete':

            if print_off:
                print ('Deleting file ' + str(path))
            try:
                conn.client.delete_object( Bucket = bucket, Key = path )
            except:
                return False
            return True
        return False

    def get_size( self, *args, **kwargs ):

        """get the size of the path"""

        converted_size, conversion = self.get_size_path( self.bucket, self.path, self.conn, *args, **kwargs )
        self.size = converted_size
        self.size_units = conversion

    @staticmethod
    def get_size_path( bucket: str, path: str, conn: aws_connections.Connection, 
                        **kwargs ) -> Tuple[ float, str ]:

        """get the size of the S3Path"""

        response = conn.client.head_object(Bucket = bucket, Key = path)
        bytes = response['ContentLength']
        return do.convert_bytes( bytes, **kwargs )

    def write( self, *args, **kwargs):
       
        temp_Path = do.Path( self.filename )
        temp_Path.write( **kwargs )

        self.upload( local_Path = temp_Path )

        temp_Path.remove( override = True )

    def create( self, *args, string = '', **kwargs ):

        self.write( string = '', **kwargs )

    def read( self, *args, **kwargs):

        temp_Path = do.Path( self.filename )
        self.download( local_Path = temp_Path )

        contents = temp_Path.read( **kwargs )

        temp_Path.remove( override = True )
        return contents


class S3Dirs( do.Dirs ):

    def __init__( self ):

        do.Dirs.__init__( self )
        self.PATHS_CLASS = S3Paths
        self.DIR_CLASS = S3Dir

class S3Paths( S3Dirs ):


    def __init__( self ):

        S3Dirs.__init__( self )
        self.PATHS_CLASS = S3Paths
        self.DIR_CLASS = S3Dir



