### User profile import
import user_profile_import
user_profile = user_profile_import.init()

#import the s3_funcs module
import boto3_funcs as b3f
import s3_funcs as s3f

###import credentials
creds = b3f.import_credentials( user_profile.aws_creds_path,  user_profile.aws_roles['s3temp'] )
print (creds)

### Insert your own s3_functions to test
bucket = 'aee-analytics-tools-dev-in-il'
prefix = 'Data_Team_Test/'

s3f.list_buckets()
s3f.list_subfolders( bucket, prefix )
