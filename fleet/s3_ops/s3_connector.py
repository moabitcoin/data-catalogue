import boto3
from fleet.configs import drive_config as config

logger = config.get_logger(__name__)


boto3.set_stream_logger(name=config.S3TRANSFER_LOG_NAME,
                        level=config.S3TRANSFER_LOG_LEVEL)

boto3.set_stream_logger(name=config.BOTO3_LOG_NAME,
                        level=config.BOTO3_LOG_LEVEL)

boto3.set_stream_logger(name=config.BOTOCORE_LOG_NAME,
                        level=config.BOTOCORE_LOG_LEVEL)


class S3ConnectorError(Exception):
  pass


class S3Connector(object):

  def __init__(self):

    self.session = None
    self.s3_resource = None
    self.s3_bucket = None

  def connect(self, env_name=None):

    try:

      if env_name is None:
        env_name, profile_name = self.get_credentials()
      else:
        profile_name = config.AWS_PROFILE_NAMES[env_name]

      self.bucket_name = config.AWS_DRIVE_DATA_BUCKETS[env_name]

      self.session = boto3.Session(profile_name=profile_name)
      self.s3_resource = self.session.resource('s3')
      self.s3_bucket = self.s3_resource.Bucket(self.bucket_name)

    except Exception as err:

      logger.error('Error setting up S3 connector, {}'.format(err))
      raise S3ConnectorError('unable to connect to S3. Environment set up? (data-catalogue env set)')

  def get_credentials(self):

    with open(config.AWS_CREDENTIALS_FILE) as pfile:
      creds = pfile.readlines()
    creds = [c.strip() for c in creds]

    return creds

  def close(self):

    try:

      pass

    except Exception as err:

      logger.error('Error setting up S3 connector, {}'.format(err))

  def is_alive(self):

    return self.s3_bucket is not None

  def put_folder(self, source_dir, dest_dir):

    pass
    # walk throught the source folder and put each file or object on S3

  def put_file(self, file_path, s3_key):

    try:

      self.s3_resource.meta.client.upload_file(file_path,
                                               self.bucket_name,
                                               s3_key)

      return True

    except Exception as err:

      logger.error('Error putting file on S3'
                   ': {}, {} , {}'.format(file_path, s3_key, err))
      return False

  def delete_file(self, s3_key):

    try:

      obj = self.s3_resource.Object(self.bucket_name, s3_key)
      obj.delete()
      return True

    except Exception as err:

      logger.error('Error deleting file on S3 : {}, {}'.format(s3_key, err))
      return False

  def list_objects(self, s3_key):

    # TODO : This should move to get_paginator

    obj_list = []
    ContToken = None

    try:
      s3_list_objects = self.s3_resource.meta.client.list_objects_v2
      kwargs = {'Bucket': self.bucket_name,
                'Prefix': s3_key}

      while True:

        resp = s3_list_objects(**kwargs)

        new_objects = [obj['Key'] for obj in resp['Contents']]
        obj_list.extend(new_objects)

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

      return obj_list

    except Exception as err:

      logger.error('Error fetching file on S3 : {}, {}'.format(s3_key, err))
      return []

  def list_subdirs(self, s3_key):

    sub_dirs = []

    try:

      kwargs = {'Bucket': self.bucket_name, 'Delimiter': '/', 'Prefix': s3_key}
      p = self.s3_resource.meta.client.get_paginator('list_objects')

      page_iterator = p.paginate(**kwargs)

      sub_dirs = [prefix.get('Prefix') for page in page_iterator
                  for prefix in page.get('CommonPrefixes')]

      return sub_dirs

    except Exception as err:

      logger.error('Error fetching file on S3 : {}, {}'.format(s3_key, err))
      return []
