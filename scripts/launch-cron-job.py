import os
import argparse
from pathlib import Path

from crontab import CronTab
from fleet.configs import drive_config as cfg

logger = cfg.get_logger(__name__)


def build_cron_job(source, destination, enviroment):

  scripts_path = Path(__file__).parent.resolve()
  repo_path = scripts_path.parent
  cron_cmd = cfg.CRONJOB_WAYLENS_CMD

  waylens_cron_cmd = cron_cmd.format(scripts_path, source, destination,
                                     repo_path, enviroment)

  cron = CronTab(tabfile='/etc/crontab', user=False)
  logger.info('Cron Job adding : {}'.format(waylens_cron_cmd))
  job = cron.new(command=waylens_cron_cmd, user=os.environ['USER'])
  job.hour.on(21)
  cron.write_to_user(user=os.environ['USER'])


if __name__ == '__main__':

  parser = argparse.ArgumentParser('Launching cron job on waylens drives')

  parser.add_argument('-s', dest='drive_source', type=Path, required=True,
                      help='Drive data source (exported)')
  parser.add_argument('-d', dest='drive_destination', type=Path, required=True,
                      help='Drive data destination (transformed)')
  parser.add_argument('-e', dest='enviroment', type=str, required=True,
                      help='Conda virtual enviroment')

  args = parser.parse_args()

  build_cron_job(args.drive_source, args.drive_destination, args.enviroment)
