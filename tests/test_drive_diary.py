from collections import namedtuple
from unittest import TestCase
from fleet.s3_ops.diary import Diary


class TestDriveDiary(TestCase):

  def test_fetch_diary_exists(self):

    diary = Diary()
    vars = 'command dest exclude op repo token avro_schema_version'
    action_tuple = namedtuple('ops', vars)
    kwargs = {'command': 'diary',
              'dest': '/tmp/',
              'exclude': None, 'op': 'fetch', 'avro_schema_version': 'v3',
              'repo': 'dump', 'token': '5764d602-7b06-443a-b748-0d57d9870c57'}
    ops = action_tuple(**kwargs)
    self.assertTrue(diary.run(ops))

  def test_fetch_diary_does_not_exists(self):

    diary = Diary()
    vars = 'command dest exclude op repo token avro_schema_version'
    action_tuple = namedtuple('ops', vars)
    kwargs = {'command': 'diary',
              'dest': '/tmp/',
              'exclude': None, 'op': 'fetch', 'avro_schema_version': 'v3',
              'repo': 'dump', 'token': '5764d602-7b06-443a-b748-0d57d9870c5?'}
    ops = action_tuple(**kwargs)
    self.assertFalse(diary.run(ops))
