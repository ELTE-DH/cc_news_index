import sys

from boto3 import client
from botocore.exceptions import ReadTimeoutError, IncompleteReadError, ResponseStreamingError, ClientError

from create_index import parse_args

"""
Write the list of warc.gz files to the STDOUT
"""


def main():
    aws_access_key_id, aws_secret_access_key, key_name, *_, out_dir, _ = parse_args()

    # Check key name
    if key_name == '-':
        print('A single key name must be specified!', file=sys.stderr)
        exit(1)

    # Login to S3
    s3 = client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Download file
    try:
        s3.download_file('commoncrawl', key_name, out_dir / key_name)
    except (ClientError, ReadTimeoutError, IncompleteReadError, ResponseStreamingError) as e:
        print(f'ClientError/ReadTimeoutError/IncompleteReadError/ResponseStreamingError '
              f'(probably rate limited)', e, key_name, file=sys.stderr)
        exit(1)


if __name__ == '__main__':
    main()
