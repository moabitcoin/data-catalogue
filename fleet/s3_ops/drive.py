from fleet.configs import drive_config


class Drive(object):

  def __init__(self):

    self.uuid = None

  def _fetch(self, args):

    self._set_uuid(args.token)
    pass

  def _info(self, args):

    self._set_uuid(args.token)
    pass

  def _set_uuid(self, token):

    assert token is not None, "Drive token(s) cannot be None"
    self.uuid = token

  def build_parser(self, parser):

    parser.add_argument('-t', '--token', help='Drive token (UUID)',
                        required=True)
    parser.add_argument('-d', '--dest', dest='dest', required=True,
                        help='Destination where to write the drive')

    subparsers = parser.add_subparsers(title='Drive Operations', dest='op',
                                       description='Valid drive operation',
                                       help='Select a command to run')
    fetch = subparsers.add_parser('fetch', help='Fetch drive from S3/Postgres')
    info = subparsers.add_parser('info', help='Get info on drive \
                                 from Postgres')

    fetch.add_argument('-r', '--repo', dest='repo', required=True,
                       choices=['dump', 'master'],
                       help='Repo to fetch datda from')
    fetch.add_argument('-e', '--exclude', dest='exclude',
                       choices=drive_config.DRIVE_DATA_SENSORS,
                       help='Excluded sensor list from fetched drive')
    fetch.set_defaults(main=self._fetch)

    info.add_argument('duration', action='store_true',
                      help='Get duration of the drive')
    info.add_argument('trace', action='store_true',
                      help='Get GPS trace of the drive')
    info.add_argument('compression', action='store_true',
                      help='Get image compression applied to drive')
    info.add_argument('sensor_suite', action='store_true',
                      help='Get available sensor suit description')
    info.add_argument('-d', '--dest', required=False,
                      help='Destination where to write the drive (json)')
    info.set_defaults(main=self._info)
