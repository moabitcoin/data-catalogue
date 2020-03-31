import os
import uuid
from unittest import TestCase
import fleet.configs.drive_config as config
from fleet.s3_ops.s3_connector import S3Connector


class TestS3Connection(TestCase):

  def test_s3_connection_is_up(self):

    s3_connector = S3Connector()
    s3_connector.connect(env_name='testing')

    self.assertTrue(s3_connector.is_alive())

  def test_s3_connection_is_down(self):

    s3_connector = S3Connector()
    s3_connector.connect(env_name='undefined')

    self.assertFalse(s3_connector.is_alive())

  def test_s3_put_file_exists(self):

    s3_connector = S3Connector()
    s3_connector.connect(env_name='testing')

    testfile_basename = 'drive_diary-' + uuid.uuid4().hex + '.json'
    test_file = '/tmp/' + testfile_basename

    with open(test_file, 'w') as pfile:
      pfile.write('Testing S3 connection with dummy file' + uuid.uuid4().hex)

    s3_key = os.path.join(config.AWS_DRIVE_DATA_REPOS['scratch'],
                          'test/' + testfile_basename)

    self.assertTrue(s3_connector.put_file(test_file, s3_key))

  def test_s3_put_file_doesnot_exists(self):

    s3_connector = S3Connector()
    s3_connector.connect(env_name='testing')

    testfile_basename = 'drive_diary-' + uuid.uuid4().hex + '.json'
    test_file = '/tmp/' + testfile_basename

    s3_key = os.path.join(config.AWS_DRIVE_DATA_REPOS['scratch'],
                          'test/' + testfile_basename)

    self.assertFalse(s3_connector.put_file(test_file, s3_key))

  def test_s3_fetch_obj_exists(self):

    s3_connector = S3Connector()
    s3_connector.connect(env_name='testing')

    testfile_basename = 'drive_diary-' + uuid.uuid4().hex + '.json'
    test_file = '/tmp/' + testfile_basename

    with open(test_file, 'w') as pfile:
      pfile.write('Testing S3 connection with dummy file' + uuid.uuid4().hex)

    s3_key = os.path.join(config.AWS_DRIVE_DATA_REPOS['scratch'],
                          'test/' + testfile_basename)

    _ = s3_connector.put_file(test_file, s3_key)

    s3_objects = s3_connector.list_objects(s3_key)

    self.assertTrue(s3_key in s3_objects)
    self.assertTrue(s3_objects != [])

  def test_s3_fetch_obj_does_not_exists(self):

    s3_connector = S3Connector()
    s3_connector.connect(env_name='testing')

    testfile_basename = 'drive_diary-' + uuid.uuid4().hex + '.json'

    s3_key = os.path.join(config.AWS_DRIVE_DATA_REPOS['scratch'],
                          'test/' + testfile_basename)

    s3_objects = s3_connector.list_objects(s3_key)

    self.assertTrue(s3_objects == [])

  def test_s3_list_directory(self):

    s3_connector = S3Connector()
    s3_connector.connect(env_name='testing')

    s3_key = config.AWS_DRIVE_DATA_REPOS['scratch']
    sub_dirs = s3_connector.list_subdirs(s3_key)
    expected_s3_dir = os.path.join(s3_key, 'test/')

    self.assertTrue(expected_s3_dir in sub_dirs)
