import os
import uuid
import argparse
import random
from shutil import copyfile
from fleet.utils.mock_drive_data_gen import build_mock_diary, \
    build_mock_drive, build_mock_sequence, build_mock_element, \
    build_mock_data, build_mock_sensor, build_mock_autonomy, save_table_dump, \
    build_mock_annotation, mock_image_annotation, \
    add_to_list, add_to_table
from fleet.utils.helpers import read_file_list, read_vid, \
    read_info_json, read_config_file, get_drive_chunks, \
    read_avro_schemas
from fleet.utils.map_updates import build_mock_collection, \
    build_mock_feature, add_feature_to_collection
from fleet.configs import drive_config as config


logger = config.get_logger(__name__)

sensor_vendors = {"camera": "flir", "imu": "you-m-u",
                  "gnss": "darpa", "vehicle": "daimler"}

sensor_fps = {"camera": 20, "imu": 10, "gnss": 10, "vehicle": 25}


def get_source_path(cfgs, source_path, source_type):

  base_path = cfgs["destination"]["base_path"]
  fmt = cfgs["destination"][source_path].format(source_type)
  return os.path.join(base_path, fmt)


def build_sensor_suite(data_token, data_time_stamp, vid_path, frame_index,
                       lat_lon, speed, choices):

  sensor_list = []

  sensor_measurements = {"camera": {"cam_codec": "raw",
                                    "cam_is_resized": False,
                                    "cam_im_height": 1280, "cam_im_width": 640,
                                    "bsens_seq_filename": vid_path,
                                    "bsens_seq_frame": frame_index,
                                    "cam_seq_number": frame_index,
                                    "cam_time_stamp": data_time_stamp,
                                    "cam_exposure_time_us": 100,
                                    "cam_gain_level_dB": -8.0},
                         "imu": {"imu_acc": [0.1, 0.1, 0.1],
                                 "imu_mag": [1.0, 1.0, 1.0],
                                 "imu_gyro": [1.0, 1.0, 1.0],
                                 "imu_temp": 39.0},
                         "gnss": {"gnss_loc": lat_lon, "gnss_mode": "DGPS",
                                  "gnss_speed": speed, "gnss_heading": 94.0,
                                  "gnss_loc_error": 0.1,
                                  "gnss_speed_error": 1.0,
                                  "gnss_heading_error": 5.0},
                         "vehicle": {"vehicle_odometer": frame_index + 0.0,
                                     "vehicle_steering_angle": 90.0,
                                     "vehicle_speed": speed}}
  for choice in choices:

    fps = sensor_fps[choice]

    mock_sensor = build_mock_sensor(data_token=data_token,
                                    data_timestamp=data_time_stamp,
                                    sensor_modality_type=choice,
                                    sensor_sampling_frequency=fps,
                                    sensor_vendor_info=sensor_vendors[choice],
                                    sensor_hw_uuid=str(uuid.uuid4()),
                                    sensor_available=True)
    _ = [add_to_table(mock_sensor, key, value)
         for key, value in sensor_measurements[choice].items()]

    sensor_list.append(mock_sensor)

  return sensor_list


def build_mock_drive_diary(cfgs, avro_schema_path):

  model_version = "https://gitlab.mobilityservices.io/am/roam/perception" + \
      "/ad-kit/models/raw/41ebbde6383949249ab085c84b3b695d.pb"

  drive_table = {}

  raw_chunk = 0
  raw_chunk_id = 0
  ts_error = cfgs["timestamp_uncertainity"]

  vid_list = read_file_list(cfgs["vid_list"])
  info_list = read_file_list(cfgs["info_list"])
  avro_schemas = read_avro_schemas(avro_schema_path)
  # hacky mapping till we solve naming convention with M
  schema_mapping = {"drive_diary": "diary",
                    "drive": "drive",
                    "sequence": "sequence",
                    "element": "element",
                    "data": "data",
                    "sensordata": "sensor"}

  assert len(vid_list) == len(info_list), "#vids and #jsons must match"

  logger.info("Parsing {} jsons for {} vids".format(len(vid_list),
                                                    len(info_list)))
  diary_table_path = get_source_path(cfgs, "tables", "drive_diary")
  drive_diary, diary_date = build_mock_diary("LongS3Path", location="mocked",
                                             drive_tokens=[])

  drive_id = diary_date + "_" + drive_diary["diary_token"]
  vehicle_id = "vehicle_" + drive_diary["vehicle_id"]
  # v<avro_schema_major_version>/<yyyy>-<mm>-<dd>_<uuid>/vehicle_<id>/drives
  drives_folder = os.path.join(cfgs["destination"]["base_path"],
                               drive_id, vehicle_id, "drives")
  diary_table_path = os.path.join(cfgs["destination"]["base_path"],
                                  drive_id, vehicle_id)

  feature_collection = build_mock_collection(drive_diary["diary_token"])

  for vid_path, json_path in zip(vid_list, info_list):

    ts, lat_lon, speed = read_info_json(json_path, cfgs, None)
    ts = [timestamp * cfgs["timestamp_multiplier"] for timestamp in ts]

    if None in [ts, lat_lon, speed]:
      logger.warning("Skipping {}".format(vid_path))
      continue

    drive = build_mock_drive(lat_lon, ts, sequence_tokens=[],
                             is_mapping=False, route_tokens=[])
    add_to_list(drive_diary, "drive_tokens", drive["drive_token"])
    add_to_table(drive, "diary_token", drive_diary["diary_token"])
    add_to_list(drive, "route_tokens", drive["drive_token"])

    drive_data = list(zip(ts, lat_lon, speed))
    chunks = get_drive_chunks(drive_data, chunk_size=cfgs["chunk_size"])

    drive_folder = os.path.join(drives_folder, drive["drive_token"])
    os.makedirs(drive_folder, exist_ok=True)

    for chunk_id, chunk in enumerate(chunks):

      elements_table = []
      data_table = []
      sensor_table = []

      (ts, lat_lon, speed) = zip(*chunk)
      lat_lon = [list(l) for l in lat_lon]

      sequence = build_mock_sequence(ts, lat_lon, element_tokens=[])
      add_to_table(sequence, "drive_token", drive["drive_token"])
      add_to_list(drive, "sequence_tokens", sequence["sequence_token"])

      sequence_dir = "{0:06d}_{1}".format(chunk_id, sequence["sequence_token"])

      sequences_dir = os.path.join(drive_folder, "sequences", sequence_dir)
      os.makedirs(sequences_dir, exist_ok=True)

      os.makedirs(os.path.join(sequences_dir, "video"), exist_ok=True)
      copyfile(vid_path, os.path.join(sequences_dir, "video/video.mov"))

      mock_vid_path = os.path.join(sequences_dir, "video/video.mov")

      for idx, _ in enumerate(speed):

        elemen_kwargs = {"element_location": lat_lon[idx],
                         "autonomous_engaged": True}

        elements = [build_mock_element(ts[idx] + random.randint(ts_error[0],
                                                                ts_error[1]),
                                       **elemen_kwargs)
                    for _ in range(len(cfgs["async_split"]))]

        data_kwargs = {"data_blob": mock_vid_path, "data_format": "mov",
                       "data_loc": lat_lon[idx], "sensor_tokens": [],
                       "osm_feature_id": None, "hdmap_feature_id": None}

        datas = [build_mock_data(e["timestamp"], **data_kwargs)
                 for e in elements]

        _ = [add_to_list(sequence, "element_tokens", e["element_token"])
             for e in elements]

        sensor_choices = list(cfgs["async_sensor_prefix"])

        for e, d, count in zip(elements, datas, cfgs["async_split"]):

          autonomy_args = {"element_token": e["element_token"],
                           "autonomy_model_version": model_version}

          autonomy = build_mock_autonomy(**autonomy_args)

          add_to_table(e, "autonomy_token", autonomy["autonomy_token"])
          add_to_table(e, "data_token", d["data_token"])
          add_to_table(e, "sequence_token", sequence["sequence_token"])
          add_to_table(d, "element_token", e["element_token"])

          choices = random.sample(sensor_choices, count)

          # we consider sensor triggered with cam as sync with iteself
          add_to_table(e, "sync", "camera" in choices)

          mock_sensors = build_sensor_suite(d["data_token"],
                                            d["timestamp"],
                                            vid_path, idx, lat_lon[idx],
                                            speed[idx], choices)

          _ = [sensor_choices.remove(c) for c in choices]

          _ = [add_to_list(d, "sensor_tokens", m["sensor_token"])
               for m in mock_sensors]

          _ = [sensor_table.append(m)
               for m in mock_sensors]

          data_table.append(d)
          elements_table.append(e)

      save_table_dump("sequence", {'sequence': [sequence]},
                      sequences_dir, avro_schemas[schema_mapping["sequence"]])
      save_table_dump("data", {'data': data_table},
                      sequences_dir, avro_schemas[schema_mapping["data"]])
      save_table_dump("element", {'element': elements_table},
                      sequences_dir, avro_schemas[schema_mapping["element"]])
      save_table_dump("sensordata", {'sensor_data': sensor_table},
                      sequences_dir,
                      avro_schemas[schema_mapping["sensordata"]])

    save_table_dump("drive", {"drive": [drive]},
                    drive_folder, avro_schemas[schema_mapping["drive"]])
    logger.info("Done with {}".format(vid_path))

  save_table_dump("drive_diary", {'diary': [drive_diary]},
                  diary_table_path,
                  avro_schemas[schema_mapping["drive_diary"]])
  logger.info("Done writing diary {} at {}".format(drive_diary["diary_token"],
                                                   diary_table_path))


if __name__ == "__main__":

  parser = argparse.ArgumentParser("Building Async mock"
                                   "drive data from BDD100K")

  parser.add_argument("-c", "--config_file", dest="config_file", type=str,
                      help="YAML config file for mocking")
  parser.add_argument("-s", "--schema_path", dest="schema_path", type=str,
                      help="Path to avro schema files")

  args = parser.parse_args()

  cfgs = read_config_file(args.config_file)

  build_mock_drive_diary(cfgs, args.schema_path)
