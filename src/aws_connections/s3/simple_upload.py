import user_profile_import
user_profile = user_profile_import.init()

import s3_funcs
aws_role = '721818040399_aap-s3temp-ic-uiuc'

creds = s3_funcs.import_credentials( user_profile.aws_creds_path, aws_role )

bucket = 'aa-userland-s3-nonprd'
s3_key = 'datalabs/dfga/shared/network_planning/8760.pbix'
filepath = 'D:/Users/E169709/Desktop/8760.pbix'

s3_funcs.upload_file( bucket, s3_key, filepath )
