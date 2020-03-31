import os
import uuid
import datetime
import itertools
import collections
from os.path import join as osp
from pathlib import Path

import tqdm
import ffmpeg
from numpy import trapz
from prettytable import PrettyTable
from avro.schema import Parse as schema_parser
from avro.io import Validate as validate
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from fleet.configs import drive_config as config

logger = config.get_logger(__name__)


def build_token():
  return str(uuid.uuid4())


class DriveDataGenerator:

  def __init__(self, schema_path):

    pobj = Path(schema_path)

    if not pobj.is_dir():
      logger.error('Error: reading schema files'
                   '- path does not exist: '.format(schema_path))

    self.schema_dict = self.read_schemas(schema_path)

    self.collection = {name: collections.OrderedDict()
                       for name in config.DRIVE_DATA_AVRO_TABLE_NAMES}

    self.gnss_hertz = 10.0
    self.imu_hertz = 10.0
    self.vehicle_hertz = 10.0

    self.cam_vendor = 'Waylens'
    self.gnss_vendor = 'Waylens'
    self.imu_vendor = 'Waylens'
    self.vehicle_vendor = 'Waylens'

    self.cam_codec = 'h264'
    self.hardware_token = build_token()

    self.drive_table_info = PrettyTable()
    self.drive_table_info.field_names = ['vehicle', 'date', 'diary_token',
                                         'drive_token', '#sequences',
                                         'distance(KM)', 'time(hh:mm:ss)']

  def get_diary(self, drive_date):

    return self.collection['diary'].get(drive_date)

  def add_diary(self, date, vehicle_id, log_path='', location='Berlin'):

    diary = self.collection['diary'].get(date)

    if diary:
      return diary

    diary = self.make_drive_diary([], date, vehicle_id, log_path, location)
    self.collection['diary'][date] = diary

    return diary

  def add_drive_to_diary(self, diary, is_mapping=False):

    drive = self.make_drive(diary['diary_token'], [], [], -1, -1, is_mapping)

    self.collection['drive'][drive['drive_token']] = drive
    diary['drive_tokens'].append(drive['drive_token'])

    return drive

  def add_sequence_to_drive(self, drive):

    drive_token = drive['drive_token']
    sequence = self.make_sequence(drive_token, [], None, None, None, None)

    self.collection['sequence'][sequence['sequence_token']] = sequence
    drive['sequence_tokens'].append(sequence['sequence_token'])

    return sequence

  def add_element_to_sequence(self, sequence, geo_loc, unix_ts, sync=False,
                              autonomy_token=None, annotation_token=None):

    sequence_token = sequence['sequence_token']

    element = self.make_element(sequence_token, None, geo_loc,
                                sync, '', '', unix_ts)

    self.collection['element'][element['element_token']] = element
    sequence['element_tokens'].append(element['element_token'])

    return element

  def add_data_to_element(self, element, geo_loc, unix_ts,
                          blob='', format='mpeg'):

    element_token = element['element_token']

    data = self.make_data(element_token, unix_ts, geo_loc, blob, format)

    self.collection['data'][data['data_token']] = data
    element['data_token'] = data['data_token']

    return data

  def build_sensor(self, **kwagrs):

    return {k: v for k, v in kwagrs.items()}

  def add_sensor_cam(self, data, seq_nr, cam_time_stamp, vid_info,
                     video_fn, available):

    sensor = self.build_sensor(data_token=data['data_token'],
                               sensor_token=build_token(),
                               sensor_modality_type='camera',
                               sensor_vendor_info=self.cam_vendor,
                               sensor_available=available,
                               sensor_hw_uuid=self.hardware_token,
                               sensor_sampling_frequency=float(vid_info['fps']),
                               data_timestamp=data['timestamp'],
                               cam_time_stamp=cam_time_stamp,
                               cam_seq_number=None,
                               cam_codec=vid_info['codec'],
                               cam_is_resized=False,
                               cam_exposure_time_us=None,
                               cam_gain_level_db=None,
                               cam_im_height=vid_info['height'],
                               cam_im_width=vid_info['width'],
                               bsens_seq_filename=video_fn,
                               bsens_seq_frame=seq_nr)

    self.collection['sensor_data'][sensor['sensor_token']] = sensor
    data['sensor_tokens'].append(sensor['sensor_token'])

    return sensor

  def add_sensor_gnss(self, data, gnss_loc, gnss_heading, gnss_loc_err,
                      gnss_speed, available=True):

    sensor = self.build_sensor(data_token=data['data_token'],
                               sensor_token=build_token(),
                               sensor_modality_type='gnss',
                               sensor_vendor_info=self.gnss_vendor,
                               sensor_available=available,
                               sensor_hw_uuid=self.hardware_token,
                               sensor_sampling_frequency=self.gnss_hertz,
                               data_timestamp=data['timestamp'],
                               gnss_mode='standard',
                               gnss_loc=gnss_loc,
                               gnss_speed=gnss_speed,
                               gnss_heading=gnss_heading,
                               gnss_loc_error=gnss_loc_err,
                               gnss_speed_error=None,
                               gnss_heading_error=None)

    self.collection['sensor_data'][sensor['sensor_token']] = sensor
    data['sensor_tokens'].append(sensor['sensor_token'])

    return sensor

  def add_sensor_imu(self, data, imu_acc, imu_gyro, available=True):

    sensor = self.build_sensor(data_token=data['data_token'],
                               sensor_token=build_token(),
                               sensor_modality_type='imu',
                               sensor_vendor_info=self.imu_vendor,
                               sensor_available=available,
                               sensor_hw_uuid=self.hardware_token,
                               sensor_sampling_frequency=self.imu_hertz,
                               data_timestamp=data['timestamp'],
                               imu_acc=imu_acc,
                               imu_mag=None,
                               imu_gyro=imu_gyro,
                               imu_temp=None)

    self.collection['sensor_data'][sensor['sensor_token']] = sensor
    data['sensor_tokens'].append(sensor['sensor_token'])

    return sensor

  def add_sensor_vehicle(self, data, vehicle_speed, available=True):

    sensor = self.build_sensor(data_token=data['data_token'],
                               sensor_token=build_token(),
                               sensor_modality_type='vehicle',
                               sensor_vendor_info=self.vehicle_vendor,
                               sensor_available=available,
                               sensor_hw_uuid=self.hardware_token,
                               sensor_sampling_frequency=self.vehicle_hertz,
                               data_timestamp=data['timestamp'],
                               vehicle_odometer=None,
                               vehicle_steering_angle=None,
                               vehicle_speed=vehicle_speed)

    self.collection['sensor_data'][sensor['sensor_token']] = sensor
    data['sensor_tokens'].append(sensor['sensor_token'])

    return sensor

  def make_drive_diary(self, drive_token_list, date, vehicle_id,
                       log_path='', location='germany'):

    diary = {'diary_token': build_token(),
             'vehicle_id': vehicle_id,
             'diary_date': date,
             'diary_log': log_path,
             'location': location,
             'drive_tokens': drive_token_list}

    return diary

  def make_drive(self, diary_token, sequence_token_list, geo_loc_list,
                 ts_start, ts_stop, is_mapping=False):
    drive = {'diary_token': diary_token,
             'drive_token': build_token(),
             'drive_locations': geo_loc_list,
             'drive_count': len(geo_loc_list),
             'timestamp_start': ts_start,
             'timestamp_stop': ts_stop,
             'sequence_tokens': sequence_token_list,
             'is_mapping': is_mapping,
             'route_tokens': []}
    return drive

  def make_sequence(self, drive_token, element_token_list, ts_start, ts_stop,
                    loc_start, loc_stop):

    sequence = {'drive_token': drive_token,
                'sequence_token': build_token(),
                'timestamp_start': ts_start,
                'timestamp_stop': ts_stop,
                'sequence_loc_start': loc_start,
                'sequence_loc_stop': loc_stop,
                'element_tokens': element_token_list}

    return sequence

  def make_element(self, sequence_token, data_token, geo_loc, sync,
                   autonomy_token, annotation_token, unix_ts):

    autonomous_engaged = False
    if autonomy_token is not None:
      autonomous_engaged = True

    element = {'sequence_token': sequence_token,
               'autonomy_token': autonomy_token,
               'autonomous_engaged': autonomous_engaged,
               'element_token': build_token(),
               'annotation_token': annotation_token,
               'data_token': data_token,
               'timestamp': unix_ts,
               'element_location': geo_loc,
               'sync': sync}

    return element

  def make_data(self, element_token, unix_ts, geo_loc=None,
                data_blob=None, data_format='mpeg'):

    data = {'timestamp': unix_ts,
            'element_token': element_token,
            'data_token': build_token(),
            'data_blob': data_blob,
            'data_format': data_format,
            'sensor_tokens': [],
            'data_loc': geo_loc,
            'osm_feature_id': None,
            'hdmap_feature_id': None}

    return data

  def read_schemas(self, schema_path):

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

  def update_diary(self, diary_avro_file):

    return self.save_avro(self.collection['diary'], 'diary',
                          'diary', diary_avro_file)

  def save_single(self, table_name, token, schema_name, avro_file):

    table = {table_name: self.collection[table_name][token]}
    return self.save_avro(table, table_name, schema_name, avro_file)

  def save_collection(self, table_name, token_list, schema_name,
                      avro_file_path):

    table = {token: self.collection[table_name][token] for token in token_list}
    return self.save_avro(table, table_name, schema_name, avro_file_path)

  def make_table(self, table_name, table):

    return {table_name: list(table.values())}

  def save_avro(self, table, table_name, schema_name, avro_file_path):

    table_dict = self.make_table(table_name, table)
    schema = self.schema_dict[schema_name]

    assert validate(schema, table_dict), \
        'Error validating with avro schema {}'.format(schema_name)

    with DataFileWriter(open(avro_file_path, 'wb'),
                        DatumWriter(), schema) as writer:
        writer.append(table_dict)

  def save_drive(self, vid_path, vid_meta, drive, drives_dest):

    drive_token = drive['drive_token']
    drive_path = os.path.join(drives_dest, drive_token)

    sequence_tokens = drive['sequence_tokens']
    pbar = tqdm.trange(len(sequence_tokens))

    logger.info('Writing drive at {}'.format(drive_path))

    for idx in pbar:
      sequence_token = sequence_tokens[idx]

      sequence_suffix = '{0:06}_{1}'.format(idx, sequence_token)
      seq_dest = osp(drive_path, 'sequences', sequence_suffix)
      seq_vid_dir = osp(seq_dest, 'video')

      os.makedirs(seq_vid_dir, exist_ok=True)
      sequence_vid_path = osp(seq_vid_dir, 'video.mp4')

      self.save_sequence(vid_path, vid_meta, sequence_vid_path,
                         sequence_token, seq_dest)
      pbar.set_description('Sequence {}/{}: {}'.format(idx + 1,
                                                       len(sequence_tokens),
                                                       sequence_token))

    drive_avro = osp(drives_dest, drive['drive_token'], 'drive.avro')
    self.save_single('drive', drive['drive_token'], 'drive', drive_avro)

  def save_sequence(self, vid_path, vid_info, sequence_vid_path,
                    sequence_token, sequence_dest):

    try:

      sequence = self.collection['sequence'][sequence_token]

      element_tokens = sequence['element_tokens']
      data_tokens = [self.collection['element'][element_token]['data_token']
                     for element_token in element_tokens]
      sensor_tokens = [self.collection['data'][data_token]['sensor_tokens']
                       for data_token in data_tokens]
      sensor_tokens = list(itertools.chain(*sensor_tokens))

      sequence_avro = osp(sequence_dest, 'sequence.avro')
      elements_avro = osp(sequence_dest, 'element.avro')
      data_avro = osp(sequence_dest, 'data.avro')
      sensor_avro = osp(sequence_dest, 'sensordata.avro')

      self.save_sequence_video(sensor_tokens, vid_path,
                               vid_info, sequence_vid_path)
      self.update_sequence(sensor_tokens, 'bsens_seq_filename',
                           sequence_vid_path)

      self.save_single('sequence', sequence_token, 'sequence', sequence_avro)
      self.save_collection('element', element_tokens, 'element', elements_avro)
      self.save_collection('data', data_tokens, 'data', data_avro)
      self.save_collection('sensor_data', sensor_tokens, 'sensor', sensor_avro)

    except Exception as err:
      logger.error('Error saving sequence {}, {}'.format(sequence_token, err))

  def update_sequence(self, sensor_tokens, key, vid_path):

    table = [self.collection['sensor_data'][token] for token in sensor_tokens]
    table = [t for t in table if t['sensor_modality_type'] == 'camera']

    for t in table:
      t[key] = vid_path

  def get_sequence_frames(self, vid_reader, token_list):

    table = [self.collection['sensor_data'][token] for token in token_list]
    table = [t for t in table if t['sensor_modality_type'] == 'camera']

    framed_indices = [t['bsens_seq_frame'] for t in table]

    return [next(vid_reader) for _ in framed_indices]

  def save_sequence_video(self, sensor_tokens, vid_path,
                          vid_info, vid_sequence_path):

    table = [self.collection['sensor_data'][token] for token in sensor_tokens]
    table = [t for t in table if t['sensor_modality_type'] == 'camera']

    frame_indices = [t['bsens_seq_frame'] for t in table]

    start_frame = frame_indices[0]
    end_frame = frame_indices[-1]

    logger.debug('Sequence vid begin : {} (incl.),'
                 ' vid end : {} (incl.)'.format(start_frame, end_frame))

    start_time = round(start_frame / vid_info['fps'], 3)
    num_frames = end_frame - start_frame + 1

    vid_args = {'ss': start_time}
    vid_sequence_args = {'c:v': 'copy', 'c:a': 'copy', 'frames': num_frames}

    vid = ffmpeg.input(vid_path, **vid_args)
    vid_sequence = ffmpeg.output(vid, vid_sequence_path, **vid_sequence_args)
    vid_sequence.run(capture_stdout=True, capture_stderr=True)

    for idx, t in enumerate(table):
      t['bsens_seq_frame'] = idx

  def add_drive_stats(self, vehicle, diary_date, diary_token,
                      drive, speeds, times):

    ts_begin = times.min()
    ts_end = times.max()

    dist = trapz(speeds, times)

    distance = '{0:.2f}'.format(dist / 1000.0)
    time_string = str(datetime.timedelta(seconds=round(ts_end - ts_begin, 0)))

    drive_token = drive['drive_token']
    sequence_tokens = drive['sequence_tokens']

    self.drive_table_info.add_row([vehicle, diary_date, diary_token,
                                   drive_token, len(sequence_tokens),
                                   distance, time_string])

  def show_drive_stats(self):

    logger.info(self.drive_table_info)

  def clear_drive(self):

    self.collection['drive'].clear()
    self.collection['sequence'].clear()
    self.collection['element'].clear()
    self.collection['data'].clear()
    self.collection['sensor_data'].clear()
