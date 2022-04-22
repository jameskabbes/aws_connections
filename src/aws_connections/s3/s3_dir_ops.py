from __future__ import annotations
import dir_ops as do
import py_starter as ps
import aws_connections
from aws_connections import s3

import functools
from typing import Tuple, List

def set_connection( **kwargs ):

    DEFAULT_KWARGS = aws_connections.cred_dict
    joined_kwargs = ps.merge_dicts( DEFAULT_KWARGS, kwargs )

    s3.conn = aws_connections.Connection( 's3', **joined_kwargs )

def download_wrap( method ):

    @functools.wraps( method )
    def wrapper( self, *args, override: bool = False, print_off: bool = False, 
                    Destination = None, destination = None, **kwargs  ):

        #whether this is a Dir or a Path, create the parents before copying        
        if Destination == None:
            if self.type_path:
                Destination = do.Path( destination )
            if self.type_dir:
                Destination = do.Dir( destination )

        Destination.create_parents()

        #check to make sure the user wants to do this
        if not override:
            do.print_to_from( True, 'Downloading', str(self), str(Destination) )
            override = ps.confirm_raw( string = '' )

        # perform the actual method        
        if override:
            do.print_to_from( print_off, 'Downloading', str(self), str(Destination) )

            if method( self, *args, destination = Destination.path, override=override, print_off=print_off, **kwargs ):
                return True

        return False

    return wrapper

def upload_wrap( method ):

    @functools.wraps( method )
    def wrapper( self, *args, override: bool = False, print_off: bool = False, 
                    Destination = None, destination = None, **kwargs  ):

        if Destination == None:
            if self.type_path:
                Destination = do.Path( destination )
            if self.type_dir:
                Destination = do.Dir( destination )

        #check to make sure the user wants to do this
        if not override:
            do.print_to_from( True, 'Uploading', str(Destination), str(self) )
            override = ps.confirm_raw( string = '' )

        # perform the actual method        
        if override:
            do.print_to_from( print_off, 'Uploading', str(Destination), str(self) )

            if method( self, *args, destination = Destination.path, override=override, print_off=print_off, **kwargs ):
                return True

        return False

    return wrapper

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

        do.Dir.__init__( self, self.path )
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
    def exists_dir( bucket: str, path: str, conn: aws_connections.Connection, **kwargs ):
        
        return 'CommonPrefixes' in conn.client.list_objects_v2( Bucket = bucket, Prefix = path, Delimiter = '/', MaxKeys=1 )

    @staticmethod
    def get_size_dir( bucket: str, path: str, conn: aws_connections.Connection,
                    *args, **kwargs ):

        self = S3Dir( bucket = bucket, path = path, conn = conn )
        Paths_inst = self.list_contents_Paths( block_dirs=True, block_paths=False )

        Paths_inst.print_atts()

        bytes = 0
        for Path_inst in Paths_inst:
            Path_inst.get_size( conversion = None )
            bytes += Path_inst.size 
        
        return do.convert_bytes( bytes, **kwargs )

    @staticmethod
    def remove_dir( bucket: str, path: str, conn: aws_connections.Connection,
                    *args, **kwargs ):

        try:
            conn.resource.Bucket( bucket ).objects.filter( Prefix = path ).delete()
        except:
            return False
        return True

    @staticmethod
    def copy_dir( bucket: str, path: str, conn: aws_connections.Connection, *args, 
                    destination: str = '', destination_bucket = None, **kwargs ):

        remote_Dir = S3Dir( bucket = bucket, path = path )
        remote_Paths = remote_Dir.walk_contents_Paths( block_dirs=True )

        for remote_Path in remote_Paths:
            rel_Path = remote_Path.get_rel( remote_Dir )
            destination_path = do.join( destination, rel_Path.path )
            remote_Path.copy( *args, destination = destination_path, **kwargs ) 

    @upload_wrap
    @do.inherited_instance_method
    def upload( self, *args, **kwargs ):
        pass

    @staticmethod
    def upload_dir( bucket: str, path: str, conn: aws_connections.Connection, *args,
                        destination: str = '', **kwargs ):
        
        local_Dir = do.Dir( destination )
        local_Paths = local_Dir.walk_contents_Paths( block_dirs=True )

        for local_Path in local_Paths:
            rel_Path = local_Path.get_rel( local_Dir )
            remote_Path = S3Path( bucket = bucket, path = do.join( path, rel_Path.path ) )
            remote_Path.upload( *args, Destination = local_Path, **kwargs )

    @download_wrap
    @do.inherited_instance_method
    def download( self, *args, **kwargs ):
        pass
    
    @staticmethod
    def download_dir( bucket: str, path: str, conn: aws_connections.Connection, *args,
                        destination: str = '', **kwargs ):

        remote_Dir = S3Dir( bucket = bucket, path = path )
        remote_Paths = remote_Dir.walk_contents_Paths( block_dirs=True )
        local_Dir = do.Dir( destination )
        
        for remote_Path in remote_Paths:
            rel_Path = remote_Path.get_rel( remote_Dir )
            local_Path = do.Path( local_Dir.join( rel_Path.path ) )
            remote_Path.download( *args, Destination = local_Path, **kwargs )
        
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

        response = self.conn.client.list_objects_v2(Bucket = self.bucket, Prefix = prefix, Delimiter = '/', **kwargs)

        filenames = []

        # 1. Add all files immediately underneath
        try:
            for file_dict in response['Contents']:
                filenames.append(file_dict['Key'])
        except:
            #print ('S3 Location ' + str(self.uri) + ' does not contain any files')
            pass

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

    def create_parents(self):
        pass


class S3Path( S3Dir, do.Path ):

    STATIC_METHOD_SUFFIX = '_path'
    INSTANCE_METHOD_ATTS = ['bucket','path','conn']

    def __init__( self, *args, **kwargs ) :

        S3Dir.__init__( self, *args, **kwargs )
        do.Path.__init__( self, self.path )
        #S3Dir.__init__( self, *args, **kwargs )

        self.DIR_CLASS = S3Dir
        self.PATH_CLASS = S3Path
        self.DIRS_CLASS = S3Dirs
        self.PATHS_CLASS = S3Paths

    def print_imp_atts(self, **kwargs):

        return self._print_imp_atts_helper( atts = ['uri','dirs','ending','size'], **kwargs )

    @staticmethod
    def exists_path(bucket: str, path: str, conn: aws_connections.Connection, *args, **kwargs):

        try:
            conn.resource.Object( bucket, path ).load()
        except:
            return False
        return True

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
    def remove_path( bucket: str, path: str, conn: aws_connections.Connection, 
                        override: bool = False, print_off: bool = True ) -> bool:

        """deletes file at path: BE CAREFUL"""

        try:
            conn.client.delete_object( Bucket = bucket, Key = path )
        except:
            return False
        return True

    @staticmethod
    def get_size_path( bucket: str, path: str, conn: aws_connections.Connection,
                        **kwargs ) -> Tuple[ float, str ]:

        """get the size of the S3Path"""

        response = conn.client.head_object(Bucket = bucket, Key = path)
        bytes = response['ContentLength']
        return do.convert_bytes( bytes, **kwargs )

    @staticmethod
    def write_path( *static_method_args, **kwargs):

        """write to a local file, then upload to S3"""

        temp_Path = do.Path( 'TEMP' )
        temp_Path.write( **kwargs )

        S3Path.upload_path( *static_method_args, local_Path = temp_Path )

        temp_Path.remove( override = True )

    @staticmethod
    def create_path( *static_method_args, string = '', **kwargs):

        """write a blank string to the file location"""

        S3Path.write_path( *static_method_args, string = string, **kwargs )

    @staticmethod
    def read_path( *static_method_args, **kwargs ):

        """download the s3 file to a local path and read the contents"""

        temp_Path = do.Path( 'TEMP' )
        S3Path.download_path( *static_method_args, local_Path = temp_Path )

        contents = temp_Path.read( **kwargs )
        temp_Path.remove( override = True )
        
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


class S3Dirs( do.Dirs ):

    INSTANCE_METHOD_ATTS = ['bucket','path','conn']

    def __init__( self ):

        do.Dirs.__init__( self )
        self.DIR_CLASS = S3Dir
        self.PATH_CLASS = S3Path
        self.DIRS_CLASS = S3Dirs
        self.PATHS_CLASS = S3Paths

class S3Paths( S3Dirs ):

    INSTANCE_METHOD_ATTS = ['bucket','path','conn']

    def __init__( self ):

        S3Dirs.__init__( self )
        self.DIR_CLASS = S3Dir
        self.PATH_CLASS = S3Path
        self.DIRS_CLASS = S3Dirs
        self.PATHS_CLASS = S3Paths



