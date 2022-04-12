import aws_connections
aws_connections._Dir.print_atts()

import aws_connections.s3 as s3f
print ( s3f.add_s3n_to_key( 'TEST_S3_KEY' ) )