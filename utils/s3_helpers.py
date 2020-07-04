import boto3
import config
import logging
import re

log = logging.getLogger(__name__)

# GLOBAL DATA (AWS CLIENT)
aws_session = boto3.Session(aws_access_key_id=config.Access.AWS_KEY,
                            aws_secret_access_key=config.Access.AWS_SECRET)

s3_client = aws_session.client('s3', region_name=config.AlphaVantage.S3_REGION_NAME)


def list_keys(Bucket, Prefix='', Suffix='', full_path=True, remove_ext=False):
    # get pages for bucket and prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=Bucket, Prefix=Prefix)

    # iterate through pages and store the keys in a list
    keys = []
    for page in page_iterator:
        if 'Contents' in page.keys():
            for content in page['Contents']:
                key = content['Key']
                if not key.endswith('/'):  # ignore directories
                    if key.endswith(Suffix):
                        if not full_path:
                            key = re.sub(Prefix, '', key)
                        if remove_ext:
                            key = re.sub('\.[^.]+$','',key)
                        keys.append(key)
    return keys
