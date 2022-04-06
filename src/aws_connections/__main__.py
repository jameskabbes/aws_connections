import sys
sys_args = sys.argv[1:]

from aws_connections.aws_connections import run
run( *sys_args )

