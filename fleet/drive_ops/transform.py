from fleet.configs import drive_config as config
from fleet.utils.helpers import list_dir as ls
from fleet.utils import oneplus_transform as oneplus
from fleet.utils import waylens_transform as waylens
from fleet.configs import waylens_config as waylens_cfg

logger = config.get_logger(__name__)


class Transform:

    def __init__(self):

      pass

    def _transform_zed(self, args):

      if args.source == args.destination:
        return logger.error('source and destination path must be different')

      vehicle = args.vehicle
      source = args.source
      destination = args.destination

      logger.info('Transforming ZED data drives')
      logger.info('Not implemented yet')

    def _transform_oneplus(self, args):

      if args.source == args.destination:
        return logger.error('source and destination path must be different')

      vehicle = args.vehicle
      source = args.source
      destination = args.destination

      logger.info('Transforming OnePlus drives')

      try:

        xml_fn, xml_prefix = ls(source, ".xml", with_prefix=True)
        video_fn, vid_prefix = ls(source, ".mp4", with_prefix=True)

        assert len(video_fn) == len(xml_fn), 'Expected {0} vids for {1} XMLs '
        'files {0} vids ≠ {1} XML'.format(len(video_fn), len(xml_fn))

        assert set(vid_prefix) == set(xml_prefix), 'Expected matching '
        'XML files for each video for OnePlus drive'

        vids = list(zip(vid_prefix, video_fn))
        xmls = list(zip(xml_prefix, xml_fn))

        logger.info('Found {} Oneplus drives to process'.format(len(vids)))

        vids.sort(key=lambda v: v[0])
        xmls.sort(key=lambda c: c[0])

        drives = [(v[1], x[1]) for v, x in zip(vids, xmls)]
        logger.info('Destination path: {}'.format(destination))

        oneplus.transform_drives(vehicle, drives, destination)

      except Exception as err:
        logger.error('Error processing Oneplus drives at {}, {}'.format(source,
                                                                        err))

    def _transform_waylens(self, args):

      if args.source == args.destination:
        return logger.error('source and destination path must be different')

      vehicle = args.vehicle
      source = args.source
      destination = args.destination
      push = args.push

      logger.info('Transforming Waylens data drives')

      try:

        vid_list, vid_prefix = ls(source, waylens_cfg.DRIVE_VID_EXT,
                                  with_prefix=True)
        csv_list, csv_prefix = ls(source, waylens_cfg.DRIVE_META_EXT,
                                  with_prefix=True)

        assert len(vid_list) == len(csv_list), 'Expected {0} vids for {1} csv '
        'files {0} vids ≠ {1} csv'.format(len(vid_list), len(csv_list))

        assert set(vid_prefix) == set(csv_prefix), 'Expected matching '
        'csv files for each vid for waylens drive'

        vids = list(zip(vid_prefix, vid_list))
        csvs = list(zip(csv_prefix, csv_list))

        logger.info('Found {} Waylens drives to process'.format(len(vids)))

        vids.sort(key=lambda v: v[0])
        csvs.sort(key=lambda c: c[0])

        drives = [(v[1], c[1]) for v, c in zip(vids, csvs)]

        waylens.transform_drives(vehicle, drives, destination, push)

      except Exception as err:

        logger.error('Error processing waylens drives at {}, {}'.format(source,
                                                                        err))

    def build_parser(self, parser):

      subparsers = parser.add_subparsers(title='Transform drive data',
                                         dest='hardware',
                                         description='CLI commands to transform drive data to our schema',
                                         help='Select a drive source')
      subparsers.required = True

      zed = subparsers.add_parser('zed', help='ZED camera drives')
      oneplus = subparsers.add_parser('oneplus', help='OnePlus drives')
      waylens = subparsers.add_parser('waylens', help='Waylens drives')

      zed.add_argument('-v', '--vehicle', dest='vehicle', default='hilda',
                       required=True, help='Vehicle Identification'
                       ' (Hilda, Sally, Simulation etc)')
      zed.add_argument('-s', '--source', dest='source',
                       required=True, help='Drive data source')
      zed.add_argument('-d', '--dest', dest='destination', required=True,
                       help='Destination where to write the drive')
      zed.set_defaults(main=self._transform_zed)

      oneplus.add_argument('-v', '--vehicle', dest='vehicle', default='sally',
                           required=True, help='Vehicle Identification'
                           ' (Hilda, Sally, Simulation etc)')
      oneplus.add_argument('-s', '--source', dest='source',
                           required=True, help='Drive data source')
      oneplus.add_argument('-d', '--dest', dest='destination',
                           help='Destination where to write the drive')
      oneplus.set_defaults(main=self._transform_oneplus)

      waylens.add_argument('-v', '--vehicle', dest='vehicle', default='hilda',
                           required=True, help='Vehicle Identification'
                           ' (Hilda, Sally, Simulation etc)')
      waylens.add_argument('-s', '--source', dest='source',
                           required=True, help='Drive data source')
      waylens.add_argument('-d', '--dest', dest='destination',
                           help='Destination where to write the drive')
      waylens.add_argument('-p', '--push', dest='push', default=False,
                           action='store_true', help='Push the drive after'
                           ' transforming')
      waylens.set_defaults(main=self._transform_waylens)
