import os
import argparse
import wget
import logging

url_prefix = 'http://dl.yf.io/bdd-data/bdd100k/video_parts/bdd100k_videos'
url_string = '{0}_{1}_{2:02d}.zip'

logger = logging.getLogger(__name__)
c_handler = logging.StreamHandler()
c_format = logging.Formatter('[%(name)s][%(levelname)s][%(message)s]')
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)


def fetch_data(dest, data_chunk_type='train', chunk_range=[0, 10]):

  logger.info('Fetching BDD {} data'.format(data_chunk_type))

  for idx in range(chunk_range[0], chunk_range[1]):

    try:

      source_filename = url_string.format(url_prefix, data_chunk_type, idx)

      basename = os.path.basename(source_filename)
      dest_filename = '{}/{}'.format(dest, basename)

      logger.info('Fetching {} -> {}'.format(source_filename, dest_filename))

      wget.download(source_filename, dest_filename, bar=wget.bar_thermometer)

    except Exception as err:

        logger.info('Error fetching {}, {}'.format(source_filename, err))


if __name__ == "__main__":

  parser = argparse.ArgumentParser(description='Script to fetch BDD dataset')
  parser.add_argument('-chunk_type', '-c', type=str,
                      help='Chunk type to download')
  parser.add_argument('-chunk_range', '-r', type=int, nargs='+',
                      help='Chunk range to download')
  parser.add_argument('-destination', '-d', type=str,
                      help='Location where to download the file')

  args = parser.parse_args()

  fetch_data(args.destination, args.chunk_type, args.chunk_range)
