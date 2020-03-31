from fleet.configs import drive_config


class Sequence(object):

  def __init__(self):

    self.uuid = None

  def _fetch(self, args):
    pass

  def _info(self, args):
    pass

  def _set_uuid(self, token):

    assert token is not None, "Sequence token(s) cannot be None"
    self.uuid = token

  def run(self, args):

    self._set_uuid(args.token)
    self.destination = args.dest

  def build_parser(self, parser):

    parser.add_argument('-t', '--token', help='Sequence token (UUID)',
                        required=True)
    parser.add_argument('-d', '--dest', dest='dest', required=True,
                        help='Destination where to write the sequence')
    parser.set_defaults(main=self.run)

    subparsers = parser.add_subparsers(title='Sequence Operations', dest='op',
                                       description='Valid sequence operation',
                                       help='Select a command to run')
    fetch = subparsers.add_parser('fetch',
                                  help='Fetch sequence from S3/Postgres')
    info = subparsers.add_parser('info',
                                 help='Get info on sequence from Postgres')

    fetch.add_argument('-r', '--repo', dest='repo', required=True,
                       choices=['dump', 'master'],
                       help='Repo to fetch datda from')
    fetch.add_argument('-e', '--exclude', dest='exclude',
                       choices=drive_config.DRIVE_DATA_SENSORS,
                       help='Exclude sensor from fetched sequence')
    fetch.set_defaults(main=self._fetch)

    info.add_argument('duration', action='store_true',
                      help='Get duration of the sequence')
    info.add_argument('trace', action='store_true',
                      help='Get GPS trace of the sequence')
    info.add_argument('compression', action='store_true',
                      help='Get image compression applied to sequence')
    info.add_argument('sensor_suite', action='store_true',
                      help='Get available sensor suit description')
    info.set_defaults(main=self._info)
