import os
import sys
import uuid
import json
import collections
import ntpath

from avro.schema import Parse as schema_parser
from avro.io import Validate as validate

from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import fleet.drive_ops.helpers as hps
from fleet.configs import drive_config as config
from fleet.utils.helpers import read_avro_schemas

logger = config.get_logger(__name__)


def make_token():
  return str(uuid.uuid4())


class DriveDataGenerator:
  def __init__(self, schema_path):

    if not os.path.exists(schema_path):
        sys.exit("Error: reading schema files - path does not exist: ",
                 schema_path)

    self.schema_dict = read_avro_schemas(schema_path)
    self.schema_dict_fast = {}

    self.diary_dict = collections.OrderedDict()
    self.data_dict = collections.OrderedDict()
    self.drive_dict = collections.OrderedDict()
    self.sensor_dict = collections.OrderedDict()
    self.element_dict = collections.OrderedDict()
    self.sequence_dict = (
        collections.OrderedDict()
    )  # store sequences indexed by token

    self.gps_hertz = 1.0
    self.gps_vendor = "OnePlus6T"
    self.cam_vendor = "OnePlus6T"

    self.gps_hardware_token = make_token()
    self.cam_hardware_token = make_token()

  def set_video_propeties(self, video_filename=None, width=1920,
                          height=1080, fps=30, codec="h246"):

    # logger.info("video filename: {}".format(video_filename))
    # logger.info("video width: {}".format(width))
    # logger.info("video height: {}".format(height))

    self.video_filename = video_filename
    self.cam_width = width
    self.cam_height = height
    self.cam_hertz = fps
    self.cam_codec = codec

  def get_drive_tokens(self):
    return list(self.drive_dict.keys())

  def get_drive(self, token):
    return self.drive_dict[token]

  # return all sequences tokes in one drive
  def get_sequence_tokens(self, drive_token):
    drive = self.drive_dict[drive_token]
    return drive["sequence_tokens"]

  def get_diary(self):

    keys = list(self.diary_dict.keys())
    if len(keys) > 0:
        return self.diary_dict[keys[0]]
    else:
        return None

  def get_element_tokens(self, sequence_token):
    sequence = self.sequence_dict[sequence_token]
    return sequence["element_tokens"]

  def get_data_tokens(self, element_tokens):

    data_tokens = []
    for token in element_tokens:
      el = self.element_dict[token]
      data_tokens.append(el["data_token"])
    return data_tokens

  def get_sensor_tokens(self, data_tokens):

    sensor_tokens = []
    for token in data_tokens:
      data = self.data_dict[token]
      sensor_tokens.extend(data["sensor_tokens"])
    return sensor_tokens

  def add_diary(self, date, vehicle_id="0", driver_id="0", log_path="",
                location="germany"):

    diary_row = self.make_drive_diary([], date, vehicle_id, driver_id,
                                      log_path, location)

    self.diary_dict[diary_row["diary_token"]] = diary_row

    return diary_row

  def add_drive(self, diary_parent, is_mapping=False):

    ts_start = -1
    ts_stop = -1

    # reference back to parent node
    diary_token = diary_parent["diary_token"]

    drive_row = self.make_drive(diary_token, [], [], ts_start,
                                ts_stop, is_mapping)
    self.drive_dict[drive_row["drive_token"]] = drive_row

    # and append this as child node
    assert isinstance(
        diary_parent["drive_tokens"], list
    ), "diary parents must be a list type"
    diary_parent["drive_tokens"].append(drive_row["drive_token"])

    return drive_row

  def add_sequence(self, drive_parent):

    drive_token = drive_parent["drive_token"]

    sequence_row = self.make_sequence(drive_token, [], None, None,
                                      None, None)

    self.sequence_dict[sequence_row["sequence_token"]] = sequence_row

    # and append this as child node
    assert isinstance(
        drive_parent["sequence_tokens"], list
    ), "drive_parent[sequence_tokens] is not a list type"
    drive_parent["sequence_tokens"].append(sequence_row["sequence_token"])
    return sequence_row

  def add_element(self, sequence_parent, el_loc, sync=False, autonomy_token="",
                  annotation_token="", unix_ts=None):

    if not unix_ts:
      unix_ts = hps.make_ts_now()

    sequence_token = sequence_parent["sequence_token"]

    element_row = self.make_element(sequence_token, None, el_loc, sync,
                                    autonomy_token, annotation_token, unix_ts)

    self.element_dict[element_row["element_token"]] = element_row

    # and append this as child node
    assert isinstance(
        sequence_parent["element_tokens"], list
    ), "sequence_parent[element_tokens] is not a list type"

    sequence_parent["element_tokens"].append(element_row["element_token"])

    # update start and end gps and timestamp in sequence
    self.update_sequence_start_stop_vals(sequence_parent)

    # and update start and end timestamps in drive
    drive_token = sequence_parent["drive_token"]
    drive_parent = self.drive_dict[drive_token]
    self.update_drive_start_stop_vals(drive_parent)

    return element_row

  def add_data(self, element_parent, data_loc, data_blob="",
               data_format="raw", unix_ts=None):

    if not unix_ts:
      unix_ts = hps.make_ts_now()

    # reference back to parent node
    element_token = element_parent["element_token"]

    data_row = self.make_data(element_token, [], data_loc, data_blob,
                              data_format, unix_ts)
    self.data_dict[data_row["data_token"]] = data_row

    # and this as child node
    assert (
        element_parent["data_token"] is None
    ), "element_parent[data_token] already assigned - duplicate entry"
    element_parent["data_token"] = data_row["data_token"]

    return data_row

  def add_sensor_cam(self, data_parent, bsens_seq_nr, seq_nr, cam_time_stamp,
                     video_fn=None, available=True):

    assert isinstance(data_parent, dict), "data_parent is not a dict type"
    assert isinstance(seq_nr, int), "seq_nr is not an int"
    assert isinstance(bsens_seq_nr, int), "bsens_seq_nr is not an int"
    assert isinstance(cam_time_stamp, int), "cam_time is not an int"

    modality = "camera"

    # reference back to parent node
    data_token = data_parent["data_token"]
    time_data_parent = data_parent["timestamp"]

    if not video_fn:
      video_fn = self.video_filename

    sensor_row = self.make_sensor_cam(
        data_token,
        self.cam_hardware_token,
        bsens_seq_nr,
        seq_nr,
        cam_time_stamp,
        video_fn,
        time_data_parent,
        self.cam_width,
        self.cam_height,
        modality,
        self.cam_hertz,
        self.cam_vendor,
        self.cam_codec,
        available,
    )
    self.sensor_dict[sensor_row["sensor_token"]] = sensor_row

    # and append this as child node
    assert isinstance(
        data_parent["sensor_tokens"], list
    ), "data_parent[sensor_tokens] is not a list type"
    data_parent["sensor_tokens"].append(sensor_row["sensor_token"])

    return sensor_row

  def get_drive_from_el_token(self, el_token):

    sequence_token = self.element_dict[el_token]["sequence_token"]
    drive_token = self.sequence_dict[sequence_token]["drive_token"]
    return self.drive_dict[drive_token]

  def add_sensor_gps(self, data_parent, loc_lat_lon, speed_ms, available=True):

    assert isinstance(data_parent, dict), "data_parent is not a dict type"
    assert isinstance(loc_lat_lon, list), "loc_lat_lon is not a list type"
    assert isinstance(speed_ms, float), "speed_ms is not a float"

    modality = "gnss"

    # reference back to parent node
    data_token = data_parent["data_token"]
    time_data_parent = data_parent["timestamp"]

    sensor_row = self.make_sensor_gps(
        data_token,
        self.gps_hardware_token,
        loc_lat_lon,
        speed_ms,
        time_data_parent,
        modality,
        self.gps_hertz,
        self.gps_vendor,
        available,
    )
    self.sensor_dict[sensor_row["sensor_token"]] = sensor_row

    # add as child node
    assert isinstance(
        data_parent["sensor_tokens"], list
    ), "data_parent[sensor_tokens] is not a list type"
    data_parent["sensor_tokens"].append(sensor_row["sensor_token"])

    # add location to drive location list:
    drive = self.get_drive_from_el_token(data_parent["element_token"])

    assert isinstance(
        drive["drive_locations"], list
    ), "drive[drive_locations] is not a list type"

    drive["drive_locations"].append(loc_lat_lon)
    drive["drive_count"] = len(drive["drive_locations"])

    return sensor_row

  def get_diary_json(self):
    name = "diary"
    return self.get_json(self.diary_dict, name, name)

  def save_diary_avro(self, fn):
    schema_name = "diary"
    table_name = "diary"
    return self.save_avro(self.diary_dict, table_name, schema_name, fn)

  def get_drive_json(self, drive_token):
    name = "drive"
    short_dict = {drive_token: self.drive_dict[drive_token]}
    return self.get_json(short_dict, name, name)

  def save_drive_avro(self, fn, drive_token):
    name = "drive"
    short_dict = {drive_token: self.drive_dict[drive_token]}
    return self.save_avro(short_dict, name, name, fn)

  def get_data_json(self, data_tokens=None):
    name = "data"
    if not data_tokens:
        return self.get_json(self.data_dict, name, name)
    else:
        short_dict = {}
        for token in data_tokens:
            short_dict[token] = self.data_dict[token]
        json = self.get_json(short_dict, name, name)
        return json

  def save_data_avro(self, fn, data_tokens):
    name = "data"
    short_dict = {}
    for token in data_tokens:
        short_dict[token] = self.data_dict[token]
    return self.save_avro(short_dict, name, name, fn)

  def get_sequence_json(self, sequence_token=None):
    name = "sequence"
    json = ""
    if not sequence_token:
        json = self.get_json(self.sequence_dict, name, name)
    else:
        short_dict = {sequence_token: self.sequence_dict[sequence_token]}
        json = self.get_json(short_dict, name, name)
    return json

  def save_sequence_avro(self, fn, sequence_token):

    name = "sequence"
    short_dict = {sequence_token: self.sequence_dict[sequence_token]}
    return self.save_avro(short_dict, name, name, fn)

  def get_element_json(self, element_tokens=None):

    name = "element"
    if not element_tokens:
      return self.get_json(self.element_dict, name, name)
    else:
      short_dict = {}
      for token in element_tokens:
        short_dict[token] = self.element_dict[token]
      json = self.get_json(short_dict, name, name)
      return json

  def save_element_avro(self, fn, element_tokens):

    name = "element"
    short_dict = {}
    for token in element_tokens:
      short_dict[token] = self.element_dict[token]
    return self.save_avro(short_dict, name, name, fn)

  def get_sensor_json(self, sensor_tokens=None):

    if not sensor_tokens:
      return self.get_json(self.sensor_dict, "sensor_data", "sensor")
    else:
      short_dict = {}
      for token in sensor_tokens:
        short_dict[token] = self.sensor_dict[token]
      json = self.get_json(short_dict, "sensor_data", "sensor")
      return json

  def save_sensor_avro(self, fn, sensor_tokens):
    short_dict = {}
    for token in sensor_tokens:
      short_dict[token] = self.sensor_dict[token]
    return self.save_avro(short_dict, "sensor_data", "sensor", fn)

  def get_json(self, table, table_name, schema_name):

    table_dict = self.make_table(table_name, table)
    schema = self.schema_dict[schema_name]

    logger.info("")
    logger.info("generate json for {}".format(table_name))

    json_out = json.dumps(table_dict, indent=4, sort_keys=True)

    return json_out

  def save_avro(self, table, table_name, schema_name, file_name_out):

    table_dict = self.make_table(table_name, table)
    schema = self.schema_dict[schema_name]

    logger.info("")
    logger.info("validating {}".format(table_name))

    if validate(schema, table_dict):
      logger.info("...successful")
    else:
      json_out = json.dumps(table_dict, indent=4, sort_keys=True)
      logger.info("")
      logger.info("avro validation failed on:")
      logger.info(json_out)
      logger.info("")
      sys.exit("Error: avro validation failed on: {}".format(table_name))

    logger.info("")
    logger.info("saving {}".format(ntpath.basename(file_name_out)))

    with DataFileWriter(open(file_name_out, "wb"),
                        DatumWriter(), schema) as writer:
        writer.append(table_dict)

    logger.info("...done")

  def update_drive_start_stop_vals(self, drive):

    assert isinstance(drive, dict), "drive is not a dict type"
    assert isinstance(
        drive["sequence_tokens"], list
    ), "drive does not contain key: sequence_tokens"

    size = len(drive["sequence_tokens"])
    if size == 1:

      start_token = drive["sequence_tokens"][0]
      start_sequence = self.sequence_dict[start_token]

      assert isinstance(start_sequence, dict), \
          "start_sequence is not a dict type"

      drive["timestamp_start"] = start_sequence["timestamp_start"]
      drive["timestamp_stop"] = start_sequence["timestamp_stop"]

    elif size > 1:
      stop_token = drive["sequence_tokens"][-1]
      stop_sequence = self.sequence_dict[stop_token]

      assert isinstance(stop_sequence, dict), "stop is not a dict type"

      drive["timestamp_stop"] = stop_sequence["timestamp_stop"]

    else:
      logger.info("warning: element list size = ", size)
      return

  def update_sequence_start_stop_vals(self, sequence):

    assert isinstance(sequence, dict), "sequence is not a dict type"
    assert isinstance(
        sequence["element_tokens"], list
    ), "element_tokens is not a list type"

    size = len(sequence["element_tokens"])

    if size == 1:

      el_start_token = sequence["element_tokens"][0]
      el_start = self.element_dict[el_start_token]

      assert isinstance(el_start, dict), "el_start is not a dict type"

      sequence["timestamp_start"] = el_start["timestamp"]
      sequence["timestamp_stop"] = el_start["timestamp"]

      sequence["sequence_loc_start"] = el_start["element_location"]
      sequence["sequence_loc_stop"] = el_start["element_location"]
    elif size > 1:

      el_stop_token = sequence["element_tokens"][-1]
      el_stop = self.element_dict[el_stop_token]
      assert isinstance(el_stop, dict), "el_stop is not a dict type"
      sequence["timestamp_stop"] = el_stop["timestamp"]
      sequence["sequence_loc_stop"] = el_stop["element_location"]
    else:
      logger.info("warning: element list size = ", size)
      return

  def make_drive_diary(self, drive_token_list, date, vehicle_id=None,
                       driver_id=None, log_path="", location="germany"):

    if not vehicle_id:
        vehicle_id = make_token()
    if not driver_id:
        driver_id = make_token()

    diary = {
        "diary_token": make_token(),
        "vehicle_id": vehicle_id,
        "diary_date": date,
        "diary_log": log_path,
        "location": location,
        "drive_tokens": drive_token_list,
    }

    return diary

  def make_drive(self, diary_token, sequence_token_list, loc_list, ts_start,
                 ts_stop, is_mapping=False):
    drive = {
        "diary_token": diary_token,
        "drive_token": make_token(),
        "drive_locations": loc_list,
        "drive_count": len(loc_list),
        "timestamp_start": ts_start,
        "timestamp_stop": ts_stop,
        "sequence_tokens": sequence_token_list,
        "is_mapping": is_mapping,
        "route_tokens": [],
    }
    return drive

  def make_sequence(self, drive_token, element_token_list, ts_start, ts_stop,
                    loc_start, loc_stop):

    sequence = {
        "drive_token": drive_token,
        "sequence_token": make_token(),
        "timestamp_start": ts_start,
        "timestamp_stop": ts_stop,
        "sequence_loc_start": loc_start,
        "sequence_loc_stop": loc_stop,
        "element_tokens": element_token_list,
    }

    return sequence

  def make_element(self, sequence_token, data_token, el_loc, sync=False,
                   autonomy_token="", annotation_token="", unix_ts=None):

    if not unix_ts:
        unix_ts = hps.make_ts_now()

    autonomous_engaged = False
    if not autonomy_token:
      autonomous_engaged = True

    element = {
        "sequence_token": sequence_token,
        "autonomy_token": autonomy_token,
        "autonomous_engaged": autonomous_engaged,
        "element_token": make_token(),
        "annotation_token": annotation_token,
        "data_token": data_token,
        "timestamp": unix_ts,
        "element_location": el_loc,  # lat lon
        "sync": sync,
    }

    return element

  def make_data(self, element_token, sensor_token_list, data_loc=None,
                data_blob="", data_format="raw", unix_ts=None):

    if not unix_ts:
      unix_ts = hps.make_ts_now()

    data = {
        "timestamp": unix_ts,
        "element_token": element_token,
        "data_token": make_token(),
        "data_blob": data_blob,
        "data_format": data_format,
        "sensor_tokens": sensor_token_list,
        "data_loc": data_loc,
    }

    return data

  def make_sensor_cam(self, data_token, hw_token, bsens_seq_nr, cam_seq_nr,
                      cam_time_stamp, video_fn, time_data_parent=0, width=1920,
                      height=1080, modality="camera", hz=30, vendor="Flir",
                      codec="h264", available=True):

    sensor = {
        "data_token": data_token,
        "sensor_token": make_token(),
        "data_timestamp": time_data_parent,
        "sensor_modality_type": modality,
        "sensor_sampling_frequency": hz,
        "sensor_vendor_info": vendor,
        "sensor_hw_uuid": hw_token,
        "sensor_available": available,
        "cam_codec": codec,
        "cam_is_resized": False,
        "cam_im_height": width,
        "cam_im_width": height,
        "cam_seq_number": cam_seq_nr,
        "cam_time_stamp": cam_time_stamp,
        "bsens_seq_filename": video_fn,
        "bsens_seq_frame": bsens_seq_nr,
    }
    return sensor

  def make_sensor_gps(self, data_token, hw_token, loc_lat_lon, speed_ms,
                      time_data_parent=0, modality="gnss", hz=1.0,
                      vendor="OnePlus6T", available=True):

    sensor = {"data_token": data_token,
              "sensor_token": make_token(),
              "data_timestamp": time_data_parent,
              "sensor_modality_type": modality,
              "sensor_sampling_frequency": hz,
              "sensor_vendor_info": vendor,
              "sensor_hw_uuid": hw_token,
              "sensor_available": available,
              "gnss_loc": loc_lat_lon,
              "gnss_speed": speed_ms}

    return sensor

  def make_table(self, table_name, rows_dict):

    row_list = list(rows_dict.values())
    out = {table_name: row_list}
    return out

  def read_schemas(self, schema_path):

    if not os.path.exists(schema_path):
      raise FileNotFoundError

    filelist = os.listdir(schema_path)
    filelist = [filename for filename in filelist if
                filename.endswith('avsc')]

    schemas = {}

    for filename in filelist:
      datum_type = filename.split('_')[1]
      filepath = os.path.join(schema_path, filename)

      if os.path.exists(filepath):
        schemas[datum_type] = schema_parser(open(filepath).read())
      else:
        raise FileNotFoundError

    return schemas
