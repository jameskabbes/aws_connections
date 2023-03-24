import dir_ops as do
import os

_Dir = do.Dir( os.path.abspath( __file__ ) ).ascend()   #Dir that contains the package 

from .Connection import Connection
from .Client import Client
from . import s3 

import aws_credentials

client = Client( connection_kwargs=aws_credentials.cred_dict )

