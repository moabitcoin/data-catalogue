import os
import uuid
import json
from datetime import datetime
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from fleet.configs.drive_config import get_logger

# Explaination here :
# https://gitlab.mobilityservices.io/am/roam/perception/data-catalogue/wikis/Drive-Data-Storage-Schema
logger = get_logger(__name__)


def build_mock_diary(log_path, **kwargs):

  now = datetime.utcnow()

  diary = dict({})

  diary["diary_token"] = str(uuid.uuid4())
  diary["vehicle_id"] = str(uuid.uuid4())
  # diary["driver_id"] = str(uuid.uuid4())
  diary["diary_date"] = now.strftime("%d:%m:%Y")
  diary["diary_log"] = log_path

  for key, val in kwargs.items():
    diary[key] = val

  return diary, now.strftime("%Y-%m-%d")


def build_mock_drive(lat_lon, ts, **kwargs):

    mock_drive = dict({})
    mock_drive["drive_token"] = str(uuid.uuid4())
    mock_drive["drive_locations"] = lat_lon
    mock_drive["drive_count"] = len(lat_lon)
    mock_drive["timestamp_start"] = ts[0]
    mock_drive["timestamp_stop"] = ts[-1]

    for key, val in kwargs.items():
      mock_drive[key] = val

    return mock_drive


def build_mock_sequence(ts, lat_long, **kwargs):

    mock_sequence = dict({})
    mock_sequence["sequence_token"] = str(uuid.uuid4())
    mock_sequence["timestamp_start"] = ts[0]
    mock_sequence["timestamp_stop"] = ts[-1]
    # NB : This is error Marcus"s schema needs to correct
    # mock_sequence["sequence_locations"] = lat_long
    # mock_sequence["sequence_location"] = str(uuid.uuid4())
    mock_sequence["sequence_loc_start"] = lat_long[0]
    mock_sequence["sequence_loc_stop"] = lat_long[-1]

    for key, val in kwargs.items():
      mock_sequence[key] = val

    return mock_sequence


def build_mock_element(ts, **kwargs):

  mock_element = dict({})
  mock_element["element_token"] = str(uuid.uuid4())
  mock_element["annotation_token"] = str(uuid.uuid4())
  mock_element["timestamp"] = ts
  mock_element["sync"] = None

  for key, val in kwargs.items():
    mock_element[key] = val

  return mock_element


def build_mock_autonomy(**kwargs):

  mock_autonomy = dict({})
  mock_autonomy["autonomy_token"] = str(uuid.uuid4())

  for key, val in kwargs.items():
    mock_autonomy[key] = val

  return mock_autonomy


def build_mock_data(ts, **kwargs):

  mock_data = dict({})
  mock_data["timestamp"] = ts
  mock_data["data_token"] = str(uuid.uuid4())

  for key, val in kwargs.items():
    mock_data[key] = val

  return mock_data


def build_mock_sensor(**kwargs):

  mock_sensor = dict({})
  mock_sensor["sensor_token"] = str(uuid.uuid4())

  for key, value in kwargs.items():
    mock_sensor[key] = value

  return mock_sensor


def build_mock_annotation(**kwargs):

  mock_annotation = dict({})
  mock_annotation["annotation_token"] = str(uuid.uuid4())

  for key, value in kwargs.items():
    mock_annotation[key] = value

  return mock_annotation


def mock_image_annotation(**kwargs):

  now = datetime.utcnow().timestamp() * 1000
  mock_image_annotation = dict({})
  mock_image_annotation["timestamp"] = int(now)
  mock_image_annotation["sensor_annotation_token"] = str(uuid.uuid4())
  mock_image_annotation["annotator_id"] = str(uuid.uuid4())

  for key, value in kwargs.items():
    mock_image_annotation[key] = value

  return mock_image_annotation


def add_to_list(parent, token, child):

  assert type(parent[token]) == list, "Cannot add element in non-list type"

  parent[token].append(child)


def add_to_table(parent, token, child):

  assert parent.get(token) is None, "Clash of uuid in dataabse schema"

  parent[token] = child


def save_table_dump(table_name, meta, save_path, avro_schema):

  try:

    avro_path = os.path.join(save_path, table_name + ".avro")

    avro_file = open(avro_path, "wb")
    avro_writer = DataFileWriter(avro_file, DatumWriter(), avro_schema)
    avro_writer.append(meta)
    avro_writer.close()

  except Exception as err:
    logger.error("Error saving {}, {}".format(table_name, err))


def save_table(cfgs, table_name, meta):

  try:
    base_path = cfgs["destination"]["base_path"]
    fmt = cfgs["destination"]["tables"].format(table_name)

    save_path = os.path.join(base_path, fmt)

    with open(save_path, "w") as pfile:
      json.dump(meta, pfile)

    logger.info("Done saving {}".format(save_path))

  except Exception as err:
    logger.error("Error saving {}, {}".format(table_name, err))
