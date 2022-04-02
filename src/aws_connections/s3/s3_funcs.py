###import other analytics packages
import py_starter.py_starter as ps   
import dir_ops.dir_ops as do      

from aws_connections import boto3_funcs as b3f

def get_resource(*args, **kwargs):
    return b3f.get_resource( 's3', *args, **kwargs )

def get_client(*args, **kwargs):
    return b3f.get_client( 's3', *args, **kwargs )

def list_buckets():

    resource = get_resource()
    buckets = []
    for bucket in resource.buckets.all():
        buckets.append(bucket.name)

    print ()
    print ('Buckets: ')
    print ()
    ps.print_for_loop(buckets)

    return buckets

def list_subfolders(bucket, prefix):

    #prefix needs to end with a /
    if len(prefix) > 0:
        if prefix[-1] != '/':
            prefix = prefix + '/'

    client = get_client()
    result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
   
    subfolders = []

    try:
        for i in result.get('CommonPrefixes'):
            subfolders.append(i.get('Prefix'))
            
    except:
        pass

    return subfolders

def list_files(bucket, prefix, print_results = True):

    ### Note: Prefix needs to have a hanging slash

    client = get_client()
    response = client.list_objects_v2(Bucket = bucket, Prefix = prefix)

    filenames = []

    try:
        for file_dict in response['Contents']:
            filenames.append(file_dict['Key'])
    except:
        print ('S3 Location ' + str(prefix) + ' does not exist')

    if print_results:
        print ()
        print ('Files: ')
        print()
        ps.print_for_loop( filenames )

    return filenames

def upload_file(bucket, key, local_path):

    #key is the entire aws path
    #local_path is the base path of the file
    resource = get_resource()
    resource.meta.client.upload_file(local_path, bucket, key)

def download_file(bucket, key, local_path):

    #key is the entire aws path
    #local_path is the base path of the file
    resource = get_resource()
    resource.meta.client.download_file(bucket, key, local_path)

def get_total_size_of_subfolder(bucket, prefix):

    files = list_files(bucket, prefix)

    total = 0
    for file in files:
        total += get_file_size(bucket, file, conversion = 'GB')

    print ()
    print ('Total: ' + str(total) + ' GB')

    return total

def get_file_size(bucket, key, conversion = None):

    client = get_client()
    response = client.head_object(Bucket = bucket, Key = key)
    bytes = response['ContentLength']

    converted_bytes = do.convert_bytes( bytes, conversion )
    return converted_bytes

def add_s3n_to_key(key):

    return 's3://' + key

def s3_url_to_bucket_and_key( s3_url ):

    '''Given an s3_url (next line), return the bucket and S3 key'''

    s3_url = s3_url[5:]
    dirs = do.path_to_dirs( s3_url )

    bucket = dirs[0]
    key = do.join( *dirs[1:] )

    return bucket, key

def bucket_and_key_to_s3_url( bucket, key ):

    '''Given a bucket and a key, return the s3_url'''

    s3_url = 's3://' + bucket + '/' + key
    return s3_url

def delete_file(bucket, key, override = False, print_off = False):

    client = get_client()

    inp = 'delete'
    if not override:
        inp = input( 'To delete ' + str(key) + ' from ' + bucket + ', type "delete" : ' )

    if inp == 'delete':

        if print_off:
            print ('Deleting ' + key + '...')
        client.delete_object(Bucket = bucket, Key = key)

def import_credentials( path, role, set_creds = True):

    return b3f.import_credentials( path, role, set_creds = set_creds )
