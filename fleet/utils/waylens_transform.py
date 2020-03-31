import os
import subprocess as sp
from datetime import datetime

import pandas as pd

from fleet.drive_ops.helpers import read_video_info
from fleet.configs import drive_config as config
from fleet.configs import waylens_config as waylens
from fleet.utils import waylens_drive_data_gen as waylens_drive


logger = config.get_logger(__name__)


def recreate_timestamp(vid_info, drive_info):

  """This functions updates the video creation creation_timestamp in vid_info
    Waylens's macOS application when fusinig chunk of videos together
    overwrite the creation_timestamp (time of recording) with the machine time
    (time of exporting videos)

    Args:
      vid_info: Video info dictionary
      drive_info: Drive info pandas table

    Returns:
      Video info dictionary with creation_timestamp updated from drive_info
  """

  epoc_times = drive_info[waylens.DRIVE_EPOC_TIME]
  play_times = drive_info[waylens.DRIVE_PLAY_TIME]
  frame_indices = drive_info[waylens.DRIVE_FRAME_INDEX]

  vid_info['creation_timestamp'] = epoc_times[0] \
      if frame_indices[0] == 0 else epoc_times[0] - play_times[0]

  return vid_info


def get_drive_info(vid_path, csv_path):

  """This function read the video meta data and drive meta data given
    video and csv paths. The video meta dats is exported as a dict object
    The drive meta data is read into a pandas table

    Args:
      vid_path : Video path (absolute)
      csv_path : Meta data CSV path (absolute)

    Returns:
      Video info dictionary
      Drive info as a pandas table
  """

  gnss_speed_imperial = waylens.DRIVE_GNSS_SPEED_IMP
  gnss_speed_metric = waylens.DRIVE_GNSS_SPEED_MET
  obd_speed_imperial = waylens.DRIVE_OBD_SPEED_IMP
  obd_speed_metric = waylens.DRIVE_OBD_SPEED_MET
  kph_multiplier = waylens.DRIVE_KPH_MULTIPLIER
  mph_multiplier = waylens.DRIVE_MPH_MULTIPLIER
  imu_acc_x = waylens.DRIVE_IMU_ACC_X
  imu_acc_y = waylens.DRIVE_IMU_ACC_Y
  imu_acc_z = waylens.DRIVE_IMU_ACC_Z
  imu_acc_multiplier = waylens.DRIVE_IMU_MULTIPLIER
  frame_indices = waylens.DRIVE_FRAME_INDEX

  logger.info('Vid : {}'.format(vid_path))
  logger.info('CSV : {}'.format(csv_path))

  video_meta_data = read_video_info(vid_path)

  drive_meta_data = pd.read_csv(csv_path, skiprows=[0, 1], header=0)
  drive_meta_data.drop_duplicates(frame_indices, keep='last', inplace=True)
  drive_meta_data.reset_index(inplace=True)

  logger.info('Read metadata from {}'.format(csv_path))

  df_column = drive_meta_data.columns.tolist()

  assert gnss_speed_imperial in df_column or gnss_speed_metric in df_column, \
      'Either {} or {} expected in df column'.format(gnss_speed_imperial,
                                                     gnss_speed_metric,
                                                     df_column)
  assert obd_speed_imperial in df_column or obd_speed_metric in df_column, \
      'Either {} or {} expected in df column'.format(obd_speed_imperial,
                                                     obd_speed_metric,
                                                     df_column)
  gnss_speed_units = gnss_speed_metric if gnss_speed_metric in df_column else \
      gnss_speed_imperial
  obd_speed_units = obd_speed_metric if obd_speed_metric in df_column else \
      obd_speed_imperial

  logger.info('GNSS speed units : {}'.format(gnss_speed_units))
  logger.info('OBD-II speed units : {}'.format(obd_speed_units))

  gnss_speed_multiplier = kph_multiplier \
      if gnss_speed_units == gnss_speed_metric else mph_multiplier
  odb_speed_multiplier = kph_multiplier \
      if obd_speed_units == obd_speed_metric else mph_multiplier

  drive_meta_data[gnss_speed_units] *= gnss_speed_multiplier
  drive_meta_data[obd_speed_units] *= odb_speed_multiplier
  drive_meta_data[imu_acc_x] *= imu_acc_multiplier
  drive_meta_data[imu_acc_y] *= imu_acc_multiplier
  drive_meta_data[imu_acc_z] *= imu_acc_multiplier

  waylens.DRIVE_GNSS_SPEED = gnss_speed_units
  waylens.DRIVE_OBD_SPEED = obd_speed_units

  video_meta_data = recreate_timestamp(video_meta_data, drive_meta_data)

  return video_meta_data, drive_meta_data


def transform_drive(ddgen, diary_date, vid_path, vid_info,
                    drive_info, drives_dest):

  time_multiplier = waylens.DRIVE_TIME_MULTIPLIER

  diary = ddgen.get_diary(diary_date)
  drive = ddgen.add_drive_to_diary(diary)

  drive_begin_ts = vid_info['creation_timestamp']
  drive_end_ts = drive_begin_ts + vid_info['duration_sec']

  seq_chunk = None

  drive['timestamp_start'] = int(drive_begin_ts * time_multiplier)
  drive['timestamp_stop'] = int(drive_end_ts * time_multiplier)

  frame_indices = drive_info[waylens.DRIVE_FRAME_INDEX]
  frame_indices_dict = {idy: idx for idx, idy in enumerate(frame_indices)}

  for frame_id in range(vid_info['num_frames']):

    try:

      ts = drive_begin_ts + frame_id * (1 / vid_info['fps'])
      frame_date = datetime.fromtimestamp(ts)

      new_sequence = seq_chunk != frame_date.minute
      seq_chunk = frame_date.minute

      if new_sequence:
        sequence = ddgen.add_sequence_to_drive(drive)

      utc_us = int(ts * time_multiplier)
      play_time_us = int(frame_id * (time_multiplier / vid_info['fps']))
      geo_loc = None

      element = ddgen.add_element_to_sequence(sequence, geo_loc, utc_us,
                                              sync=True)
      data = ddgen.add_data_to_element(element, geo_loc, utc_us)

      # If a synchronized frame
      df_idx = frame_indices_dict.get(frame_id)
      if df_idx:
        utc_s = drive_info[waylens.DRIVE_EPOC_TIME][df_idx]
        play_time_s = drive_info[waylens.DRIVE_PLAY_TIME][df_idx]
        gnss_lat = drive_info[waylens.DRIVE_GNSS_LAT][df_idx]
        gnss_long = drive_info[waylens.DRIVE_GNSS_LONG][df_idx]
        gnss_head = drive_info[waylens.DRIVE_GNSS_HEADING][df_idx]
        gnss_error = drive_info[waylens.DRIVE_GNSS_ERROR][df_idx]
        gnss_speed = drive_info[waylens.DRIVE_GNSS_SPEED][df_idx]
        imu_acc_x = drive_info[waylens.DRIVE_IMU_ACC_X][df_idx]
        imu_acc_y = drive_info[waylens.DRIVE_IMU_ACC_Y][df_idx]
        imu_acc_z = drive_info[waylens.DRIVE_IMU_ACC_Z][df_idx]
        imu_gyro_x = drive_info[waylens.DRIVE_IMU_GYRO_X][df_idx]
        imu_gyro_y = drive_info[waylens.DRIVE_IMU_GYRO_Y][df_idx]
        imu_gyro_z = drive_info[waylens.DRIVE_IMU_GYRO_Z][df_idx]
        vehicle_speed = drive_info[waylens.DRIVE_OBD_SPEED][df_idx]

        utc_us = int(utc_s * time_multiplier)
        play_time_us = int(play_time_s * time_multiplier)

        ddgen.add_sensor_gnss(data, [gnss_lat, gnss_long], gnss_head,
                              gnss_error, gnss_speed)
        ddgen.add_sensor_imu(data, [imu_acc_x, imu_acc_y, imu_acc_z],
                             [imu_gyro_x, imu_gyro_y, imu_gyro_z])
        ddgen.add_sensor_vehicle(data, vehicle_speed)

      if new_sequence:
        sequence['sequence_loc_start'] = geo_loc
        sequence['timestamp_start'] = utc_us

      sequence['sequence_loc_stop'] = geo_loc
      sequence['timestamp_stop'] = utc_us

      drive['drive_count'] = len(drive['drive_locations'])

      ddgen.add_sensor_cam(data, frame_id, play_time_us,
                           vid_info, vid_path, True)

    except Exception as err:
      logger.error('Error processing frame {}, {}'.format(frame_id, err))

  return drive


def transform_drives(vehicle, drives, destination, push):

  """This function transforms the waylens drive into drive data schema

    Args:
      drive: Path to Waylens drives (exported with macOS Waylens studio)
      destination: Location where to store the transformed drives

    Returns:
      None
  """

  ddg = waylens_drive.DriveDataGenerator(config.get_avro_schema_path())
  diary_dates = []

  for vid_path, csv_path in drives:

    try:
      vid_info, drive_info = get_drive_info(vid_path, csv_path)

    except Exception as err:
      logger.error('Could not fetch drive info for {}, {}, {}'.format(vid_path,
                                                                      csv_path,
                                                                      err))
      continue

    creation_ts = vid_info['creation_timestamp']
    diary_date = datetime.fromtimestamp(creation_ts)
    diary_date = diary_date.strftime('%Y-%m-%d')
    diary_dates.append(diary_date)

    diary = ddg.add_diary(diary_date, vehicle)
    diary_token = ddg.get_diary(diary_date)['diary_token']

    diary_path = os.path.join(destination, diary_date + '_' + diary_token)
    vehicle_path = os.path.join(diary_path, 'vehicle_' + vehicle)
    drives_path = os.path.join(vehicle_path, 'drives')

    os.makedirs(drives_path, exist_ok=True)

    try:
      drive = transform_drive(ddg, diary_date, vid_path, vid_info,
                              drive_info, drives_path)
    except Exception as err:
      logger.error('Failed to transform drive {}'.format(vid_path))
      continue

    ddg.add_drive_stats(vehicle, diary_date, diary_token, drive,
                        drive_info[waylens.DRIVE_OBD_SPEED],
                        drive_info[waylens.DRIVE_PLAY_TIME])
    ddg.save_drive(vid_path, vid_info, drive, drives_path)
    ddg.update_diary(os.path.join(vehicle_path, 'drive_diary.avro'))

    ddg.clear_drive()

  ddg.show_drive_stats()

  push_fmt = config.CRONJOB_WAYLENS_PUSH_CMD
  for diary_date in set(diary_dates):

    diary_token = ddg.get_diary(diary_date)['diary_token']
    push_cmd = push_fmt.format(diary_token, destination)

    if push:
      process = sp.Popen(push_cmd, shell=True, stdout=sp.PIPE)
      process.wait()
    else:
      logger.info('Now run : {}'.format(push_cmd))
