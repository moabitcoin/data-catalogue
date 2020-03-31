import os
import logging
import argparse
import botocore
import boto3
from boto3.session import Session
from boto3.s3.transfer import TransferConfig


# Disabling other loggers on verbosity
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


logging.basicConfig(filename='fetch-mytaxi-light.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


def get_mytax_data(bucket_name, object_name, destination):

  try:

    AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

    session = Session(aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)

    s3 = boto3.client('s3')
    s3_ssion = session.resource('s3')

    my_bucket = s3_ssion.Bucket(bucket_name)
    config = TransferConfig(max_concurrency=5)

    for s3_file in my_bucket.objects.all():

      if object_name not in s3_file.key:
        continue

      logging.info(s3_file.key)
      basename = os.path.basename(s3_file.key)
      dest_obj = os.path.join(destination, basename)

      s3.download_file(bucket_name, s3_file.key, dest_obj, Config=config)
      logging.info('Done with {} -> {}'.format(s3_file.key, dest_obj))

  except botocore.exceptions.ClientError as e:

    if e.response['Error']['Code'] == "404":
      logging.info("The object does not exist.")
    else:
      raise


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Fetch myTaxi dataset')
    parser.add_argument('-bucket_name', '-b', type=str,
                        help='S3 Bucket with objects to download')
    parser.add_argument('-object_name', '-o', type=str,
                        help='S3 Object in the bucket')
    parser.add_argument('-destination', '-d', type=str,
                        help='Location where to download the file')

    args = parser.parse_args()

    get_mytax_data(args.bucket_name, args.object_name, args.destination)
