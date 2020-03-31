import os
import re
import glob
import operator
import functools
from pathlib import Path

import tqdm
import numpy as np

from fleet.configs import drive_config as config
from fleet.s3_ops.s3_connector import S3Connector
from fleet.utils.helpers import read_avro_schemas, validate_with_schema

logger = config.get_logger(__name__)


class Diary(object):

  def __init__(self):

    self.diary_uuid = None
    self.s3_connector = None

  def _set_diary_uuid(self, diary_uuid):

    assert diary_uuid is not None, 'Diary token(s) cannot be None'
    self.diary_uuid = diary_uuid
    logger.info('Diary token {}'.format(self.diary_uuid))

  def _parse_drive_uri(self, drive_diary_uri):

    assert re.search(config.DRIVE_DIARY_URI_PATTERN, drive_diary_uri) \
        is not None, 'Expected drive diary uri as <yyyy>-<mm>-<dd>_<uuid>'

    diary_date = re.search(config.DRIVE_DIARY_DATE_PATTERN, drive_diary_uri)
    assert diary_date is not None, \
        'Diary Date not found in {}'.format(drive_diary_uri)
    diary_date = diary_date.group()

    diary_token = re.search(config.DRIVE_DIARY_TOKEN_PATTERN, drive_diary_uri)
    assert diary_token is not None, \
        'Diary Token pattern not found in {}'.format(drive_diary_uri)
    diary_token = diary_token.group()

    return diary_date, diary_token

  def _validate_drive_source(self, drive_path, diary_uuid=None):

    diary_uuid = diary_uuid if diary_uuid is not None else self.diary_uuid
    drive_diary_pattern = config.DRIVE_DIARY_DATE_PATTERN + '_' + diary_uuid

    drive_matches = [re.match(drive_diary_pattern, diary)
                     for diary in os.listdir(drive_path)]
    drive_matches = list(filter(lambda x: x is not None, drive_matches))
    drive_matches = [x.group() for x in drive_matches]

    assert(len(drive_matches)) == 1, 'Expected single drive complying ' \
        '<yyy>-<mm>-<dd>_{} at {} found None'.format(diary_uuid, drive_path)

    drive_diary_uri = drive_matches[0]
    _, diary_token = self._parse_drive_uri(drive_diary_uri)

    assert diary_uuid == diary_token, \
        'Token found under {} {} != {}'.format(drive_path,
                                               diary_token, diary_uuid)

    return drive_diary_uri

  def _validate_drive(self, drive_path):

    drive_flags = []
    schemas = read_avro_schemas(config.get_avro_schema_path())

    vehicle_uri = [x for x in drive_path.iterdir() if x.is_dir()]
    assert(len(vehicle_uri)) == 1, \
        'Expecting one vehicle id at {}'.format(drive_path.as_posix())

    vehicle_uri = vehicle_uri[0]
    vehicle_id = re.match(config.DRIVE_DATA_VEHICLE_URI,
                          os.path.basename(vehicle_uri))

    assert vehicle_id is not None, 'Expected 1 diary as {}/vehicle_<xxxx>' \
        ' found {}'.format(vehicle_uri.parent, vehicle_uri)

    vehicle_id = vehicle_id.group()
    drive_diary_avro = Path(vehicle_uri).joinpath('drive_diary.avro')
    assert drive_diary_avro.is_file(), \
        'Expected {} drive diary avro, No such file'.format(drive_diary_avro)

    avro_flag = validate_with_schema(drive_diary_avro, schemas['diary'])
    drive_flags.append(avro_flag)

    drives_dir = Path(vehicle_uri).joinpath('drives')
    assert drives_dir.is_dir(), \
        'Expected {}, Not such directory'.format(drives_dir, vehicle_uri)

    drives = [x for x in drives_dir.iterdir() if x.is_dir()]
    drives = [drive for drive in drives
              if re.match(config.DRIVE_DIARY_TOKEN_PATTERN,
                          drive.name) is not None]
    assert drives is not [], \
        'Expected at-least 1 drive uuid(s) at {} found None'.format(drives_dir)

    drive_avros = [drive.joinpath('drive.avro') for drive in drives]
    drive_avros_exist = [drive_avro.is_file() for drive_avro in drive_avros]
    assert any(drive_avros_exist), \
        "Atleast expected 1 drive.avro at {}".format(drives_dir)

    valid_drives = []

    for drive_exist, drive_avro in zip(drive_avros_exist, drive_avros):
      drive_flags.append(drive_exist)
      if not drive_exist:
        logger.warning('Expected {}, No such file'.format(drive_avro))
      else:
        avro_flag = validate_with_schema(drive_avro, schemas['drive'])
        valid_drives.append(drive_avro)

    drives = valid_drives
    valid_sequences = []

    sequences = [drive.parent.joinpath('sequences') for drive in drives]
    sequences_exist = [seq.is_dir() for seq in sequences]

    for sequence_exist, sequence in zip(sequences_exist, sequences):
      drive_flags.append(sequence_exist)
      if not sequence_exist:
        logger.warning('Expected sequence directory {},'.format(sequence))
      else:
        valid_sequences.append(sequence)

    pbar = tqdm.tqdm(valid_sequences)

    for sequence in pbar:
      pbar.set_description("Validating drive : {}".format(sequence.parent))
      sequence_uris = [x for x in sequence.iterdir() if x.is_dir()]
      valid_sequence_uris = [sequence for sequence in sequence_uris
                             if re.match(config.DRIVE_SEQUENCE_URI_PATTERN,
                                         sequence.name) is not None]

      assert len(valid_sequence_uris) > 0, \
          'Expected atleast 1 sequence of ' \
          'form XXXXXX_<uuid> at'.format(sequence)

      sequence_avro_files = [(uri.joinpath('data.avro'),
                              uri.joinpath('element.avro'),
                              uri.joinpath('sensordata.avro'),
                              uri.joinpath('sequence.avro'))
                             for uri in valid_sequence_uris]

      seqpbar = tqdm.tqdm(sequence_avro_files)

      for (data, element, sensordata, sequence) in seqpbar:

        avro_flag = validate_with_schema(data, schemas['data'])
        drive_flags.append(avro_flag)

        avro_flag = validate_with_schema(element, schemas['element'])
        drive_flags.append(avro_flag)

        avro_flag = validate_with_schema(sensordata, schemas['sensor'])
        drive_flags.append(avro_flag)

        avro_flag = validate_with_schema(sequence, schemas['sequence'])
        drive_flags.append(avro_flag)

    return drive_flags

  def _validate_drive_s3(self, repo_path, diary_uuid=None):

    drive_diary = []
    diary_uuid = diary_uuid if diary_uuid is not None else self.diary_uuid

    diary_token = re.match(config.DRIVE_DIARY_TOKEN_PATTERN, diary_uuid)
    assert diary_token is not None, \
        'Diary Token {} does not match \
         patten {}'.format(diary_uuid, config.DRIVE_DIARY_TOKEN_PATTERN)

    list_subdirs = self.s3_connector.list_subdirs
    diaries = list_subdirs(config.AWS_DRIVE_DATA_BUCKET, repo_path)

    diaries = [diary.rstrip('/') for diary in diaries]
    diaries = [os.path.split(diary)[1] for diary in diaries]

    diary_pattern = config.DRIVE_DIARY_DATE_PATTERN + '_' + diary_uuid

    for diary in diaries:
      diary_token = re.match(diary_pattern, diary)
      if diary_token is not None:
        drive_diary.append(diary_token.group())

    return drive_diary

  def _enumerate_drive(self, drive_path):

    drive_data_formats = [config.DRIVE_META_DATA_FORMAT] + \
        config.DRIVE_DATA_BLOB_FORMATS

    drive_files = [[f for f in glob.glob('{}/**/*.{}'.format(drive_path, fmt),
                                         recursive=True)]
                   for fmt in drive_data_formats]

    return functools.reduce(operator.iconcat, drive_files, [])

  def _push(self, args):

    repo_type = args.repo
    avro_schema_version = args.avro_schema_version
    source_path = args.source
    diary_token = args.token

    self._set_diary_uuid(diary_token)

    try:

      s3_push_notif = []
      repo_uris = config.get_aws_repo_uris(avro_schema_version)
      drive_data_repo = repo_uris[repo_type]

      # trailing forward slash creates os.path.join issues
      source_path = source_path + '/' \
          if not source_path.endswith('/') else source_path

      drive_diary_uri = self._validate_drive_source(source_path)

      self.s3_connector = S3Connector()
      self.s3_connector.connect(env_name=None)

      drive_diary_path = Path(source_path).joinpath(drive_diary_uri)

      vehicle_uri = [x for x in drive_diary_path.iterdir() if x.is_dir()]
      vehicle_uri = vehicle_uri[0]
      vehicle_id = re.match(config.DRIVE_DATA_VEHICLE_URI,
                            os.path.basename(vehicle_uri))

      vehicle_id = vehicle_id.group()
      drive_diary_avro = Path(vehicle_uri).joinpath('drive_diary.avro')
      self._push_obj_to_s3(drive_diary_avro, source_path, drive_data_repo,
                           verbose=True)

      drives_dir = Path(vehicle_uri).joinpath('drives')

      drives = [x for x in drives_dir.iterdir() if x.is_dir()]
      drives = [drive for drive in drives
                if re.match(config.DRIVE_DIARY_TOKEN_PATTERN,
                            drive.name) is not None]

      drive_avros = [drive.joinpath('drive.avro') for drive in drives]
      drive_sequences = [drive.joinpath('sequences') for drive in drives]
      pbar = tqdm.tqdm(list(zip(drive_avros, drive_sequences)))

      for drive_avro, drive_sequence in pbar:

        pbar.set_description('Pushing drive {}'.format(drive_avro.parent.name))

        self._push_obj_to_s3(drive_avro, source_path, drive_data_repo)

        sequence_uris = [x for x in drive_sequence.iterdir() if x.is_dir()]

        sequence_avro_files = [[uri.joinpath('data.avro'),
                                uri.joinpath('element.avro'),
                                uri.joinpath('sensordata.avro'),
                                uri.joinpath('sequence.avro')]
                               for uri in sequence_uris]

        video_dirs = [uri.joinpath('video') for uri in sequence_uris]
        video_files = [video_dir.iterdir() for video_dir in video_dirs
                       if video_dir.is_dir()]
        video_files = functools.reduce(operator.iconcat, video_files, [])
        video_files = [[v] for v in video_files if v.is_file()]

        assert len(sequence_avro_files) == len(video_files), \
            "Each sequence must have a video file"

        sequences_to_push = list(zip(video_files, sequence_avro_files))
        seqpbar = tqdm.tqdm(sequences_to_push)

        for seqdata in seqpbar:
          seqdata = functools.reduce(operator.iconcat, seqdata, [])
          seqpbar.set_description('Pushing sequence'
                                  ' {}'.format(seqdata[1].parent.name))

          databar = tqdm.tqdm(seqdata)
          for data in databar:
            databar.set_description('Pushing {}'.format(data.name))
            flag = self._push_obj_to_s3(data, source_path, drive_data_repo)
            s3_push_notif.append(flag)

          databar.close()
        seqpbar.close()

      assert np.all(s3_push_notif), \
          'Failed to push complete drive diary {}'.format(source_path)

      logger.info('Done pushing diary '
                  '{}, {}'.format(self.diary_uuid, drive_data_repo))

    except Exception as err:

      logger.error('Error pushing drive : {}'.format(err))

  def _push_obj_to_s3(self, drive_file, source_path,
                      drive_data_repo, verbose=False):

    s3_file_prefix = str(drive_file).split(source_path)[1]
    s3_file_key = os.path.join(drive_data_repo, s3_file_prefix)
    flag = self.s3_connector.put_file(str(drive_file), s3_file_key)

    if verbose:
      logger.info('Pushed {}'.format(s3_file_key))

    return True

  def _validate(self, args):

    avro_schema_version = args.avro_schema_version
    source_path = args.source
    diary_token = args.token

    self._set_diary_uuid(diary_token)

    try:

      drive_diary_uri = self._validate_drive_source(source_path)

      drive_path = Path(source_path).joinpath(drive_diary_uri)

      drive_valid_flags = self._validate_drive(drive_path)

      logger.info('Drive {} validated : {}'.format(drive_path,
                                                   all(drive_valid_flags)))

      logger.info('Now run : data-catalogue diary -t '
                  '{} push -r dump -s {}'.format(self.diary_uuid, source_path))
    except Exception as err:
      logger.error('Error validating Drive'
                   ' {}, {}'.format(self.diary_uuid, err))
      logger.error('Drive data should'
                   ' confirm with {}'.format(config.DRIVE_DATA_STRUCTURE_URL))

  def _fetch(self, args):

    repo_type = args.repo
    destination = args.dest
    diary_token = args.token
    avro_schema_version = args.avro_schema_version

    self._set_diary_uuid(diary_token)

    try:

      os.makedirs(destination, exist_ok=True)
      repo_uris = config.get_aws_repo_uris(avro_schema_version)
      drive_data_repo = repo_uris[repo_type]

      self.s3_connector = S3Connector()
      self.s3_connector.connect(env_name=None)
      s3_resource = self.s3_connector.s3_resource

      diary_uri = self._validate_drive_s3(drive_data_repo)

      if diary_uri == []:
        logger.error('Diary with token : {}, '
                     'does not exist at {}'.format(self.diary_uuid,
                                                   drive_data_repo))
        return False

      diary_s3_key = os.path.join(drive_data_repo, diary_uri[0])
      list_objects = self.s3_connector.list_objects
      drive_data_files = list_objects(config.AWS_DRIVE_DATA_BUCKET,
                                      diary_s3_key)

      drive_pbar = tqdm.tqdm(drive_data_files)

      # [TODO] : multi-thread
      for drive_file in drive_pbar:

        try:

          _, drive_suffix = drive_file.split(drive_data_repo)
          destination_file = os.path.join(destination, drive_suffix)
          destination_folder, _ = os.path.split(destination_file)

          os.makedirs(destination_folder, exist_ok=True)

          obj = s3_resource.Object(config.AWS_DRIVE_DATA_BUCKET, drive_file)
          obj.download_file('{}'.format(destination_file))
          # drive_pbar.set_description('Pulling %s' % os.path.join(drive_file))

        except Exception as err:
          logger.error('Error fetching {}, {}'.format(drive_file, err))

      logger.info('Done fetching diary with'
                  'token {} to {}'.format(self.diary_uuid, destination))
      return True

    except Exception as err:
      logger.error('Error fetching drive diary, {}'.format(err))

  def _info(self, args):

    self._set_diary_uuid(args.token)

  def build_parser(self, parser):

    parser.add_argument('-t', '--token',
                        help='Drive diary token (UUID)', required=True)

    subparsers = parser.add_subparsers(title='Diary Operations', dest='op',
                                       description='Valid diary operation',
                                       help='Select a command to run')
    fetch = subparsers.add_parser('fetch', help='Fetch drive diary to S3')
    info = subparsers.add_parser('info', help='Get info on drive diary')
    push = subparsers.add_parser('push', help='Push drive diary to S3')
    validate = subparsers.add_parser('validate', help='Validate drive'
                                     ' data structure')

    fetch.add_argument('-r', '--repo', dest='repo', required=True,
                       choices=['dump', 'master'],
                       help='Repo to fetch datda from')
    fetch.add_argument('-a', '--avro-schema-version',
                       dest='avro_schema_version',
                       default=config.get_avro_schema_version(),
                       help='Avro schema version if not'
                       'specified gets from submodule')
    fetch.add_argument('-d', '--dest', dest='dest', required=True,
                       help='Save path for drive diary on disk')
    fetch.add_argument('-e', '--exclude', dest='exclude',
                       choices=config.DRIVE_DATA_SENSORS, default=None,
                       help='Destination where to write the diary')
    fetch.set_defaults(main=self._fetch)

    info.add_argument('-a', '--avro-schema-version',
                      dest='avro_schema_version',
                      default=config.get_avro_schema_version(),
                      help='Avro schema version if not'
                      'specified gets from submodule')
    info.add_argument('duration', action='store_true', default=False,
                      help='Get duration of the diary')
    info.add_argument('trace', action='store_true', default=False,
                      help='Get GPS trace of the diary')
    info.add_argument('compression', action='store_true', default=False,
                      help='Get image compression sensor data in diary')
    info.add_argument('sensor_suite', action='store_true', default=False,
                      help='Get available sensor suit description')
    info.set_defaults(main=self._info)

    push.add_argument('-r', '--repo', dest='repo', required=True,
                      choices=['dump', 'master', 'scratch'],
                      help='Repo to fetch datda from')
    push.add_argument('-a', '--avro-schema-version',
                      dest='avro_schema_version',
                      default=config.get_avro_schema_version(),
                      help='Avro schema version if not'
                      'specified gets from submodule')
    push.add_argument('-s', '--source', dest='source', required=True,
                      help='Source of drive data on disk (uncompressed)')
    push.set_defaults(main=self._push)

    validate.add_argument('-a', '--avro-schema-version',
                          dest='avro_schema_version',
                          default=config.get_avro_schema_version(),
                          help='Avro schema version if not'
                          'specified gets from submodule')
    validate.add_argument('-s', '--source', dest='source', required=True,
                          help='Source of drive data on disk (uncompressed)')
    validate.set_defaults(main=self._validate)
