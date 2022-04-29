[GitHub Pages](https://jameskabbes.github.io/aws_connections)<br>
[PyPI](https://pypi.org/project/kabbes-aws-connections)

# aws_connections
Package for easy connections to AWS based on the boto3 library

<br> 

# Installation
`pip install kabbes_aws_connections`

<br>

# Usage
For more in-depth documentation, read the information provided on the Pages. Or better yet, read the source code.

```python
import aws_connections as aws
```

```python
aws_conn = aws.Connection( aws_access_key_id = 'XXXXXX', aws_secret_access_key = 'XXXXXX )
```

```python
remote_Path = aws.s3.S3Path( bucket = 'mybucket', path = 'path/to/file/asdf.txt', conn = aws_conn )
```

```python
remote_Path.download( destination = 'C:/Local/Path/asdf.txt' )
```

```python
remote_Path.copy( destination = 'path/to/file/asdf_copy.txt' )
```

<br>

# Author
James Kabbes
