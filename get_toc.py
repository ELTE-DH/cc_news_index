from datetime import date

from boto3 import client

from create_index import parse_args

"""
Write the list of warc.gz files to the STDOUT
"""


def main():
    aws_access_key_id, aws_secret_access_key, *_ = parse_args()

    # Login to S3
    s3 = client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # From 2016 to nowadays
    for year in range(2016, date.today().year + 1):
        for bucket_object in s3.list_objects(Bucket='commoncrawl', Prefix=f'crawl-data/CC-NEWS/{year}')['Contents']:
            key_name = bucket_object['Key']
            if not key_name.endswith('warc.gz'):
                continue
            print(key_name)


if __name__ == '__main__':
    main()
