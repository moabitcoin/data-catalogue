import os
import re
import argparse
import subprocess as sp
from pathlib import Path

from fleet.configs import drive_config as cfg

logger = cfg.get_logger(__name__)


def transform_drives_and_push(source, destination):

  myenv = os.environ

  find_cmd = cfg.CRONJOB_WAYLENS_NEW_JOBS_CMD
  transform_fmt = cfg.CRONJOB_WAYLENS_TRANSFORM_CMD
  drive_regex = cfg.CRONJOB_WAYLENS_NEW_JOBS_REGEX

  ret_val = sp.check_output(find_cmd.format(source).split())
  drive_candidates = ret_val.decode("utf-8").split()
  drive_candidates = [Path(drive) for drive in drive_candidates]
  drive_candidates = [drive for drive in drive_candidates
                      if re.search(drive_regex, drive.name)]

  logger.info('Found {} new drives'.format(len(drive_candidates)))

  for drive in drive_candidates:

    vehicle_uri = drive.parent.name
    transform_cmd = transform_fmt.format(drive, destination, vehicle_uri)
    logger.info(transform_cmd)
    process = sp.Popen(transform_cmd, shell=True, stdout=sp.PIPE)
    process.wait()


if __name__ == '__main__':

  parser = argparse.ArgumentParser('Launching cron job on waylens drives')

  parser.add_argument('-s', dest='drive_source', type=Path, required=True,
                      help='Drive data source (exported)')
  parser.add_argument('-d', dest='drive_destination', type=Path, required=True,
                      help='Drive data destination (transformed)')

  args = parser.parse_args()

  transform_drives_and_push(args.drive_source, args.drive_destination)
