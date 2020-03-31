import os
import sys
import argparse
from pathlib import Path

from avro.schema import Parse as schema_parser

from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from datetime import datetime
import dateutil.parser

import ffmpeg

import skvideo.io
import skvideo.datasets
from fleet.configs import drive_config as config

logger = config.get_logger(__name__)


# second to microsecond conversion
MICROS_IN_SEC = 1000000.0

# oneplus date format:
# <date>2019-06-13 00:46:29</date>
string_time_format = '%Y-%m-%d %H:%M:%S'


def make_ts_from_sec(sec):
  return int(sec * MICROS_IN_SEC)


def make_sec_from_ts(ts):
    return ts / MICROS_IN_SEC


def make_ts_now():

    sec = datetime.utcnow().timestamp()
    return make_ts_from_sec(sec)


def make_iso_from_ts(ts):

    sec_ts = make_sec_from_ts(ts)
    dt = datetime.fromtimestamp(sec_ts)
    return dt.strftime(string_time_format)


def make_ts_from_time_string(time_str):

    dt = datetime.strptime(time_str, string_time_format)
    ts = dt.timestamp()
    ts_long = make_ts_from_sec(ts)
    return ts_long


def read_video_info_ffmpeg(vid_path):

  probe = ffmpeg.probe(vid_path)

  info = {}

  # default values
  info["num_streams"] = 0
  info["width"] = -1
  info["height"] = -1
  info["num_frames"] = -1
  info["bitrate"] = -1
  info["codec"] = ""
  info["size_bytes"] = -1
  info["duration_sec"] = -1
  info["timebase"] = -1
  info["fps"] = -1

  info["raw"] = probe

  tb_str = None
  fr_str = None

  streams = probe.get("streams")

  if streams is not None and len(streams) > 0:
    s0 = None
    for s in streams:
      if s.get("codec_type") == "video":
        s0 = s
    if s0 is not None:
      info["num_streams"] = len(streams)
      info["width"] = int(s0.get("width", -1))
      info["height"] = int(s0.get("height", -1))
      info["num_frames"] = int(s0.get("nb_frames", -1))
      info["bitrate"] = int(s0.get("bit_rate", -1))
      info["codec"] = s0.get("codec_long_name")
      tb_str = s0.get("codec_time_base")
      fr_str = s0.get("r_frame_rate")

  pformat = probe.get("format")
  if pformat is not None:
    info["size_bytes"] = int(pformat.get("size", -1))
    info["duration_sec"] = float(pformat.get("duration", -1))

  if tb_str is not None:
    tb_str = probe["streams"][0]["codec_time_base"]
    tb_l = tb_str.split('/', 2)
    time_base = -1
    if len(tb_l) == 2:
      time_base = float(tb_l[0]) / float(tb_l[1])
      info["timebase"] = time_base

  if fr_str is not None:
    fps = int(fr_str.split('/', 2)[0])
    info["fps"] = fps

  return info


def read_video_info_raw(vid_path):

  path = Path(vid_path)

  if not path.is_file():
    logger.error("video does not exist {}".format(path.as_posix()))
    return None

  logger.info("Loading metadata for {}".format(path.as_posix()))
  ff_metadata = skvideo.io.ffprobe(path.as_posix())

  return ff_metadata


def video_count_frames(vid_path):

  videogen = skvideo.io.vreader(vid_path)
  count = 0

  # this is a generator so we have to iterate
  # inorder to count frames
  for frame in videogen:
    count += 1

  return count


def read_video_info(vid_path):

  info = {}
  mdv = None
  creation_date = None

  path = Path(vid_path)

  if not path.is_file():
    logger.error("video does not exist {}".format(path.as_posix()))
    return None

  logger.info("Loading metadata for {}".format(path.as_posix()))
  ff_metadata = skvideo.io.ffprobe(path.as_posix())

  if "video" in ff_metadata:
    mdv = ff_metadata["video"]
  else:
    logger.error("no usable metadata")
    return None

  tb_str = mdv["@time_base"]
  tb_l = tb_str.split('/', 2)
  time_base = -1

  width = int(mdv["@width"])
  height = int(mdv["@height"])
  bitrate = int(mdv['@bit_rate'])

  if len(tb_l) == 2:
    time_base = float(tb_l[0]) / float(tb_l[1])

  fr_str = mdv["@r_frame_rate"]
  fps = int(fr_str.split('/', 2)[0])

  num_frames = int(mdv.get("@nb_frames", -1))

  # time in seconds
  duration_sec = float(mdv["@duration"])
  codec = mdv["@codec_name"]

  if "tag" in mdv:
    if len(mdv["tag"]) > 0:
      if "@value" in mdv["tag"][0]:
        creation_date = mdv["tag"][0]["@value"]

  info["creation_date_iso"] = creation_date
  info["creation_timestamp"] = -1

  if creation_date is not None:
    # parse ISO 8601
    try:
      dt = dateutil.parser.parse(creation_date)
      ts = datetime.timestamp(dt)
      info["creation_timestamp"] = ts
    except Exception as err:
      logger.error("creation date cannot"
                   " be parsed: {}, {}".format(creation_date, err))
  else:
    logger.error("no creation date present")

  info["fps"] = fps
  info["timebase"] = time_base
  info["width"] = width
  info["height"] = height
  info["codec"] = codec
  info["raw"] = ff_metadata
  info["bitrate"] = bitrate

  # incase metadat doesn't contain this info
  # count frames:
  if num_frames == -1:
    logger.info("number of frames not available - counting frames")
    num_frames = video_count_frames(vid_path)
  info["num_frames"] = num_frames
  info["duration_sec"] = float(num_frames) / float(fps)

  return info


def avro_save_binary(message, schema, file_name):

    writer = DataFileWriter(open(file_name, "wb"), DatumWriter(), schema)
    writer.append(message)
    writer.close()


def avro_read_schemas(schema_path):

  path = Path(schema_path)

  if not path.is_dir():
    raise FileNotFoundError

  filelist = path.iterdir()
  filelist = [f for f in filelist if f.endswith('avsc')]

  schemas = {}

  for filepath in filelist:
    fobj = Path(filepath)
    datum_type = os.path.basename(fobj.as_posix()).split('_')[1]

    if fobj.is_file():
      with open(fobj.as_posix()) as pfile:
        schemas[datum_type] = schema_parser(pfile.read())
    else:
      logger.error('Couldn\'t avro schem file : {}'.format(filepath))

  return schemas


def main():

  parser = argparse.ArgumentParser('CMD line utiltiy for using helpers')
  parser.add_argument('-ts', '--timestamp', dest='ts', action='store_strue',
                      help='timestamp in milliseconds to iso')
  parser.add_argument('-gps', '--gps', dest='gps', action='store_strue',
                      help='oneplus gps time to ts')
  parser.add_argument('-time-arg', '--time-arg', dest='time_arg',
                      type='float', help='oneplus gps time to ts')

  args = parser.parse_args()

  flag = sys.argv[1]
  time_arg = sys.argv[2]

  if args.ts:
    logger.info("timestamp in milliseconds to iso string")
    iso = make_iso_from_ts(int(args.time_arg))
    logger.info(iso)
    return

  if args.gps:
    logger.info("gps time string to timestamp in milliseconds")
    ts = make_ts_from_time_string(args.time_arg)
    logger.info(ts)
    return


if __name__ == "__main__":
    # execute only if run as a script
    main()
