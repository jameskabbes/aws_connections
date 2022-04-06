from . import boto3_funcs
import dir_ops.dir_ops as do
import os

Dir = do.Dir( os.path.abspath( __file__ ) ).ascend()   #Dir that contains the package 
src_Dir = Dir.ascend()                                  #src Dir that is one above
repo_Dir = src_Dir.ascend()                    
cwd_Dir = do.Dir( do.get_cwd() )
