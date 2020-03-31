import os
import yaml
import json
from pathlib import Path

from avro.io import Validate
from avro.schema import Parse as schema_parser
from avro.datafile import DataFileReader
from avro.io import DatumReader
from moviepy.editor import VideoFileClip

from fleet.configs import drive_config as config

logger = config.get_logger(__name__)


def read_file_list(filepath):

  try:
    with open(filepath) as pfile:
      files = pfile.readlines()
      return [f.strip() for f in files]
  except Exception as err:
    logger.warning('Error reading {}, {}'.format(filepath, err))
    return []


def read_vid(vid_path, cfgs):

  try:

    clip = VideoFileClip(vid_path)
    new_clip = clip.resize([cfgs['im_width'], cfgs['im_height']])

    frames = [frame for frame in new_clip.iter_frames()]
    frames = [frames[idx] for idx in range(0, len(frames),
                                           cfgs['frame_sampling'])]
    return frames

  except Exception as err:
    logger.error('Error reading {}, {}'.format(vid_path, err))
    return None


def read_info_json(info_path, cfgs, size=None):

  try:

    with open(info_path, 'r') as pfile:
      data = json.load(pfile)

    drive_data = data['locations']
    drive_data = [(d['timestamp'], d['latitude'],
                   d['longitude'], d['speed']) for d in drive_data]

    if size is not None:
      drive_data = drive_data[:size]

    ts, lat, long, speed = zip(*drive_data)
    lat_lon = list(zip(lat, long))
    lat_lon = [list(d) for d in lat_lon]

    return ts, lat_lon, speed

  except Exception as err:
    logger.error('Error reading {}, {}'.format(info_path, err))


def read_config_file(cfg_file):

  try:
    with open(cfg_file, 'r') as pfile:
      cfg = yaml.load(pfile)
      logger.info('Done reading {}'.format(cfg_file))
      return cfg
  except Exception as err:
    logger.error('Error reading {}, {}'.format(cfg_file, err))


def get_drive_chunks(drive_data, chunk_size=60):

  for idx in range(0, len(drive_data), chunk_size):
    yield drive_data[idx: idx + chunk_size]


def read_avro_schemas(schema_path):

  schemas = {}
  schema_file_path = Path(schema_path)
  for datum_type in config.DRIVE_DATA_AVRO_SCHEMA_NAMES:

    try:

      avro_basename = 'avro_{}_data.avsc'.format(datum_type)
      avro_file = schema_file_path.joinpath(avro_basename)

      schemas[datum_type] = schema_parser(open(avro_file).read())
      logger.info('Read schema for {}: {}'.format(datum_type, avro_basename))

    except Exception as err:
      logger.error('Error reading schemas : {}, {}'.format(datum_type, err))

  return schemas


def validate_with_schema(avro_file_path, schema):

  try:

    with open(avro_file_path.as_posix(), 'rb') as pfile:
      datum = DataFileReader(pfile, DatumReader())
      datum = [d for d in datum]
    assert Validate(schema, datum[0]), \
        'Error validating {}'.format(avro_file_path)
    return True

  except Exception as err:
    logger.error('Error validating {}, {}'.format(avro_file_path, err))
    return False


def list_dir(source_path, ext, with_prefix=False):

  pobj = Path(source_path)

  if not pobj.is_dir():
    logger.error('Not a directory {}'.format(pobj.is_posix()))
    return None

  sources = [p for p in pobj.iterdir() if p.as_posix().endswith(ext)]
  basenames = [os.path.basename(source) for source in sources]
  prefixes = [os.path.splitext(basename)[0] for basename in basenames]

  return [sources, prefixes] if with_prefix else sources
