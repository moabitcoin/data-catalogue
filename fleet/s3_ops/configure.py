from pathlib import Path

from fleet.configs import drive_config as cfg
from fleet.configs.drive_config import get_logger

logger = get_logger(__name__)


class Configure:

  def __init__(self):

    self.environment_name = None

  def build_parser(self, parser):

    subparsers = parser.add_subparsers(title='Environment configuration',
                                       dest='op',
                                       description='Valid Environment config'
                                       ' operation', help='Select a '
                                       'command to run')

    # data-catalogue env {get, set}
    # where {get, set} is required

    subparsers.required = True

    set = subparsers.add_parser('set', help='Set environment info')
    get = subparsers.add_parser('get', help='Get environment info')

    set.set_defaults(main=self.set_environ)
    get.set_defaults(main=self.get_environ)

    set.add_argument(help='Environment configuration', dest='env',
                     choices=['default', 'develop', 'staging', 'testing'])

  def set_environ(self, args):

    self.environment_name = args.env

    assert self.environment_name in cfg.AWS_PROFILE_NAMES
    profile_name = cfg.AWS_PROFILE_NAMES[self.environment_name]
    credentials_file = cfg.AWS_CREDENTIALS_FILE
    credentials_file = Path(credentials_file)

    if credentials_file.is_file():
      credentials_file.unlink()

    credentials_file.parent.mkdir(parents=True, exist_ok=True)

    with credentials_file.open('w') as fp:
      fp.write('{}\n'.format(self.environment_name))
      fp.write('{}\n'.format(profile_name))

    logger.info('{} environment setup'.format(self.environment_name))

  def get_environ(self, args):

    if not Path(cfg.AWS_CREDENTIALS_FILE).is_file():
      logger.warning('Environment not setup')
      return

    with open(cfg.AWS_CREDENTIALS_FILE) as pfile:
      creds = pfile.readlines()

    assert len(creds) == 2, "Credentials file incorrect"
    env_name, profile_name = [c.strip() for c in creds]

    logger.info('Environment name : {}'.format(env_name))
    logger.info('Profile name : {}'.format(profile_name))
