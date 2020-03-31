import os
import re
import sys
import git
import logging
from pathlib import Path

# data-catalogue logging
CLI_LOGGING_FORMAT = '[%(filename)s][%(funcName)s:%(lineno)d]' + \
    '[%(levelname)s] %(message)s'
CLI_LOGGING_LEVEL = logging.INFO
CLI_LOGGING_STREAM = sys.stdout

# logging level for boto3

S3TRANSFER_LOG_NAME = 's3transfer'
S3TRANSFER_LOG_LEVEL = logging.WARNING

BOTO3_LOG_NAME = 'boto3'
BOTO3_LOG_LEVEL = logging.WARNING

BOTOCORE_LOG_NAME = 'botocore'
BOTOCORE_LOG_LEVEL = logging.WARNING

# ~/.aws/credentials should have 'perception' profile setup as default
AWS_CREDENTIALS_FILE = Path('~/.aws/data-catalogue-cli-credentials').expanduser()
AWS_PROFILE_NAMES = {'default': 'perception',
                     'develop': 'perception-cli-develop',
                     'testing': 'perception-cli-testing',
                     'staging': 'perception-cli-staging'}
# Perception bucket on AWS
AWS_DRIVE_DATA_BUCKETS = {'default': 'das-perception',
                          'develop': 'das-perception-develop',
                          'testing': 'das-perception-testing',
                          'staging': 'das-perception-staging'}
# Data repos for processed and unprocessed data on AWS
# dump: for unprocessed data in schema defined by HW
# master : verified data in schema defined by HW
AWS_DRIVE_DATA_REPOS = {'dump': 'perception_data/dump/drive_data/',
                        'scratch': 'perception_data/scratch/drive_data/',
                        'master': 'perception_data/master/drive_data/'}

AWS_S3_KEY_DELIMITER = '/'

# drive meta data format
DRIVE_SCHEMA_FORMAT = 'avsc'
# drive meta data format
DRIVE_META_DATA_FORMAT = 'avro'
# drive meta data version
DRIVE_SCHEMA_VERSION_REGEX = re.compile(
    r'^'                        # start of string
    r'version-'                 # version- prefix
    r'(?P<major>[0-9]+)'        # major number
    r'\.'                       # literal . character
    r'(?P<minor>[0-9+])'        # minor number
    r'\.'                       # literal . character
    r'(?P<patch>[0-9+])'        # patch number
)
DRIVE_SCHEMA_SUBMODULE = 'drive-data-configs'
# drive data blob format
DRIVE_DATA_BLOB_FORMATS = ['mov', 'mp4']
# drive data blob version
DRIVE_DATA_BLOB_VERSION = '0.1'
# Drive data URI prefix on AWS <yyyy-mm-dd>_<uuid>
DRIVE_DIARY_URI_PATTERN = '[0-9]{4}-[0-9]{2}-[0-9]{2}_' \
    '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
DRIVE_SEQUENCE_URI_PATTERN = '[0-9]{6}_' \
    '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
# drive diary date pattern in URI
DRIVE_DIARY_DATE_PATTERN = '[0-9]{4}-[0-9]{2}-[0-9]{2}'
# drive diary token pattern in URI
DRIVE_DIARY_TOKEN_PATTERN = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]' + \
    '{4}-[0-9a-f]{4}-[0-9a-f]{12}'
DRIVE_DATA_VEHICLE_URI = 'vehicle_[0-9a-zA-Z]'

DRIVE_DATA_AVRO_TABLE_NAMES = ['diary', 'drive', 'sequence',
                               'element', 'data', 'sensor_data']
DRIVE_DATA_AVRO_SCHEMA_NAMES = ['diary', 'drive', 'sequence',
                                'element', 'data', 'sensor']
DRIVE_DATA_STRUCTURE_URL = 'https://gitlab.mobilityservices.io/am/roam' \
    '/perception/data-catalogue/wikis/Drive-Data-Storage-Schema#file-structure'

# List expected to expand
DRIVE_DATA_SENSORS = ['image', 'radar', 'gps', 'speed', 'steering_wheel']

# Cron job command(s)
CRONJOB_WAYLENS_NEW_JOBS_CMD = 'find {} -type d -mmin -1440'
CRONJOB_WAYLENS_NEW_JOBS_REGEX = '[0-9]{8}'
CRONJOB_WAYLENS_TRANSFORM_CMD = 'data-catalogue transform' \
    ' waylens -s {} -d {} -v {} --push'
CRONJOB_WAYLENS_PUSH_CMD = 'data-catalogue diary -t {} push -r dump -s {}'
CRONJOB_WAYLENS_CMD = 'bash -i {}/launch-cron-job-waylens.sh {} {} {} {} > /tmp/data-catalogue.log 2>&1'


def get_logger(logger_name):

  logger = logging.getLogger(logger_name)
  logger.setLevel(CLI_LOGGING_LEVEL)
  ch = logging.StreamHandler(CLI_LOGGING_STREAM)
  formatter = logging.Formatter(CLI_LOGGING_FORMAT)
  ch.setFormatter(formatter)
  ch.setLevel(CLI_LOGGING_LEVEL)
  logger.addHandler(ch)
  logger.propagate = False

  return logger


logger = get_logger(__name__)


def get_avro_schema_version(version_type='major'):

  try:

    # [HACK] should go
    filepath = __file__
    repo_path = filepath.split('fleet')[0]

    repo = git.Repo(repo_path)
    submodules = [submodule for submodule in repo.submodules
                  if DRIVE_SCHEMA_SUBMODULE in submodule.url]
    submodules = [submodule.module().git.describe()
                  for submodule in submodules]

    if submodules is []:
      return None

    version_match = DRIVE_SCHEMA_VERSION_REGEX.search(submodules[0])

    return 'v' + version_match[version_type]

  except Exception as err:
    logger.error('Error getting avro schem version {}'.format(err))


def get_aws_repo_uris(avro_schema_version=None):

  repo_uris = {}
  avro_schema_version = get_avro_schema_version() \
      if avro_schema_version is None else avro_schema_version

  avro_schema_version += AWS_S3_KEY_DELIMITER

  for repo_type, repo_path in AWS_DRIVE_DATA_REPOS.items():
    repo_uris[repo_type] = os.path.join(repo_path, avro_schema_version)

  return repo_uris


def get_avro_schema_path():

  filepath = __file__
  repo_path = filepath.split('fleet')[0]

  repo = git.Repo(repo_path)
  submodules = [submodule for submodule in repo.submodules
                if DRIVE_SCHEMA_SUBMODULE in submodule.url]
  submodules = [submodule.abspath
                for submodule in submodules]

  if not submodules:
    return None

  return os.path.join(submodules[0], "schemas/avro")
