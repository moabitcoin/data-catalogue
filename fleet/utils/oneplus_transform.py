import os
from datetime import datetime

import ffmpeg
import xmltodict
import ntpath

import fleet.utils.oneplus_drive_data_gen as ddg
import fleet.drive_ops.helpers as hps
from fleet.configs import drive_config as config

logger = config.get_logger(__name__)


def make_loc_list(xml_doc):
  lat_lon = []

  for el in xml_doc["root"]["position"]:
    ll = [el["x_loc"], el["y_loc"]]
    lat_lon.append(ll)

  return lat_lon


def save_table(sequence_table_json, file_name):

  file = open(file_name, "w")
  file.write(sequence_table_json)
  file.close()


def get_list_of_drive_paths(oneplus_diary_path):

  path_set = set()

  file_list = os.listdir(oneplus_diary_path)

  if file_list is not None:
    for fn in file_list:
      base = ntpath.basename(fn)
      base = os.path.splitext(base)[0]
      path_set.add(base)

  base_list = []

  for b in path_set:

    base_path = os.path.join(oneplus_diary_path, b)
    fn_xml = base_path + ".xml"
    fn_mp4 = base_path + ".mp4"

    if os.path.exists(fn_xml) and os.path.exists(fn_mp4):
      base_list.append(base_path)

  return base_list


def transform_drive(gen, fn_mp4, fn_xml, out_path, max_count=None,
                    save_json=False, chunk_size_sec=60):

  info = hps.read_video_info(fn_mp4)

  creation_ts = info["creation_timestamp"]
  num_frames = info["num_frames"]
  duration_sec = info["duration_sec"]
  num_frames = info["num_frames"]
  fps = info["fps"]

  fps_check = num_frames / duration_sec
  if int(fps) != int(fps_check):
    fps = int(fps_check)

  video_base_path = ntpath.basename(fn_mp4)

  logger.info("-------------input video --------------")
  logger.info("video: " + video_base_path)
  logger.info("width: " + str(info["width"]))
  logger.info("height: " + str(info["height"]))
  logger.info("fps: " + str(fps))
  logger.info("num frames: " + str(num_frames))
  logger.info("duration (sec): " + str(duration_sec))
  logger.info("creation time: " + str(info["creation_date_iso"]))
  logger.info("---------------------------------------")

  time_step_ms = 1000000.0 / float(fps)

  base_name = "video.mp4"
  video_dir = "video/"

  gen.set_video_propeties(video_dir + base_name, info["width"], info["height"],
                          fps, info["codec"])

  diary = gen.get_diary()
  drive = gen.add_drive(diary)

  # ----------------- compute chunks ---------------------

  # time_chunk_ms = chunk_size_sec*1000000 # conver to micro-seconds
  frames_per_chunk = int(chunk_size_sec * fps)

  chunk_times = []
  frame_chunk_list = []
  frame_chunk = []

  # precompute end-time stamps for chunks
  for frame_count in range(0, num_frames, frames_per_chunk):

    frame_count_next = frame_count + frames_per_chunk
    if frame_count_next >= num_frames:
      frame_count_next = num_frames
    end_dt = int(time_step_ms * frame_count_next)

    chunk_times.append(end_dt)

    for fi in range(frame_count, frame_count_next, 1):
      ts = int(time_step_ms * fi)
      frame_chunk.append({"frame": fi, "time": ts})

    frame_chunk_list.append(frame_chunk)
    frame_chunk = []

  utc_ms_start = None
  logger.info("chunking GNSS entries ")

  gnss_chunk_list = []
  gnss_chunk = []
  chunk_index = 0

  # go through gnss list and make chunks based on chunk times
  with open(fn_xml) as fd:
    doc = xmltodict.parse(fd.read())
    count = 0
    for el in doc["root"]["position"]:

      x = el["x_loc"]
      y = el["y_loc"]
      v = el["speed"]
      t = el["time"]
      d = el["date"]

      string_time_format = "%Y-%m-%d %H:%M:%S"
      dt = datetime.strptime(d, string_time_format)
      utc_ms = hps.make_ts_from_sec(dt.timestamp())

      if count == 0:
        utc_ms_start = utc_ms
      count += 1

      loc = [float(x), float(y)]

      speed_ms = 0.0
      speed = float(v.partition(" ")[0])

      if v.find("mp/h") != -1:
        speed_ms = speed * 0.44704  # miles/h to m/s
      elif v.find("km/h") != -1:
        speed_ms = speed * 0.277778  # km/h to m/s
      elif v.find("m/s") != -1:
        speed_ms = speed

      norm_time = utc_ms - utc_ms_start
      chunk_entry = {"loc": loc, "speed": speed_ms,
                     "utc": utc_ms, "norm": norm_time}
      gnss_chunk.append(chunk_entry)

      if norm_time > chunk_times[chunk_index]:
        if chunk_index + 1 < len(chunk_times):
          chunk_index += 1
          gnss_chunk_list.append(gnss_chunk)
          gnss_chunk = []

    # append last
    gnss_chunk_list.append(gnss_chunk)

  while len(gnss_chunk_list) < len(frame_chunk_list):
    empty = []
    gnss_chunk_list.append(empty)

  # ----------------- generate schema ---------------------

  assert len(frame_chunk_list) == len(gnss_chunk_list), \
      "len(frame_chunk_list)!=len(gnss_chunk_list)"

  pre_app_loc = [-1, -1]

  for frame_chunk, gnss_chunk in zip(frame_chunk_list, gnss_chunk_list):

    # add sequence for each chunk
    sequence = gen.add_sequence(drive)

    # gnss
    for gnss_entry in gnss_chunk:

      loc = gnss_entry["loc"]
      speed_ms = gnss_entry["speed"]
      time_ms = gnss_entry["norm"]

      utc_ms = time_ms + utc_ms_start

      element = gen.add_element(sequence, loc, False, "", "", utc_ms)
      data = gen.add_data(element, loc, "", "raw", utc_ms)
      sensor = gen.add_sensor_gps(data, loc, speed_ms, True)

    # frames
    frame_size = len(frame_chunk)
    gnss_size = len(gnss_chunk)

    conv = 1
    if(gnss_size > 0):
      conv = frame_size / gnss_size

    for fc, frame_entry in enumerate(frame_chunk):

      # global frame count for entire original video file
      frame_i = frame_entry["frame"]
      time_e = frame_entry["time"]

      # time rebased to utc
      data_time = int(utc_ms_start + time_e)

      app_loc = pre_app_loc

      # find approximate location by interpolating index
      if gnss_size > 0:
        gnss_index = int(fc / conv)
        assert(gnss_index < gnss_size)
        app_loc = gnss_chunk[gnss_index]["loc"]

      element = gen.add_element(sequence, app_loc, True, "", "", data_time)
      data = gen.add_data(element, app_loc, "", "raw", data_time)

      pre_app_loc = app_loc

      # actual time as recorded in video
      cam_time = int(time_e)
      sensor = gen.add_sensor_cam(data, fc, frame_i, cam_time)

  # ----------------- save schema and video to file ---------------------

  drive_token = drive["drive_token"]

  l4_path = out_path + "/" + drive_token
  os.mkdir(l4_path)

  if save_json is True:
      save_table(gen.get_drive_json(drive_token), l4_path + "/drive.json")
  gen.save_drive_avro(l4_path + "/drive.avro", drive_token)

  l5_path = l4_path + "/" + "sequences"
  os.mkdir(l5_path)

  seq_count = 0

  if len(frame_chunk_list) != len(gen.get_sequence_tokens(drive_token)):
    logger.error("chunk list size not equal to number of sequences")
    l1 = len(frame_chunk_list)
    l2 = len(gen.get_sequence_tokens(drive_token))
    logger.error("chunk list length: " + str(l1))
    logger.error("sequence list length: " + str(l2))

  sequence_token_it = gen.get_sequence_tokens(drive_token)

  for seq_count, sequence_token in enumerate(sequence_token_it):

    l6_path = l5_path + "/" + str(seq_count).zfill(6) + "_" + sequence_token
    os.mkdir(l6_path)

    if save_json is True:
      sequence_json = gen.get_sequence_json(sequence_token)
      save_table(sequence_json, l6_path + "/sequence.json")
    gen.save_sequence_avro(l6_path + "/sequence.avro", sequence_token)

    # get all elements for this sequence
    element_tokens = gen.get_element_tokens(sequence_token)
    if save_json is True:
      element_json = gen.get_element_json(element_tokens)
      save_table(element_json, l6_path + "/element.json")
    gen.save_element_avro(l6_path + "/element.avro", element_tokens)

    # get all data for this sequence
    data_tokens = gen.get_data_tokens(element_tokens)
    if save_json is True:
      data_json = gen.get_data_json(data_tokens)
      save_table(data_json, l6_path + "/data.json")
    gen.save_data_avro(l6_path + "/data.avro", data_tokens)

    # get all sensor for this sequence
    sensor_tokens = gen.get_sensor_tokens(data_tokens)
    if save_json is True:
      sensor_json = gen.get_sensor_json(sensor_tokens)
      save_table(sensor_json, l6_path + "/sensordata.json")
    gen.save_sensor_avro(l6_path + "/sensordata.avro", sensor_tokens)

    # make video path
    l7_path = l6_path + "/" + video_dir
    os.mkdir(l7_path)
    out_fn = os.path.join(l7_path, base_name)

    frame_list = frame_chunk_list[seq_count]
    start_frame = frame_list[0]["frame"]
    end_frame = frame_list[-1]["frame"]

    logger.info("chunk video from {} to {}".format(str(start_frame),
                                                   str(end_frame)))

    start_time = round(start_frame / float(fps), 3)
    num_frames = end_frame - start_frame + 1

    in_args = {'ss': start_time}
    out_args = {'c:v': 'copy', 'c:a': 'copy', 'frames': num_frames}

    in_file = ffmpeg.input(fn_mp4, **in_args)
    output = ffmpeg.output(in_file, out_fn, **out_args)
    output.run(capture_stdout=True, capture_stderr=True)

    logger.info("check output")
    info_check = hps.read_video_info_ffmpeg(out_fn)
    num_frames_check = info_check["num_frames"]

    if num_frames_check != len(frame_chunk_list[seq_count]):
      logger.error("output video does not have correct number of frames")
    else:
      logger.info("output OK")


def transform_drives(vehicle, drives, out_path, max_count=None):

  os.makedirs(out_path, exist_ok=True)
  schema_path = config.get_avro_schema_path()

  logger.info('Schema path: {}'.format(schema_path))
  gen = ddg.DriveDataGenerator(schema_path)

  if len(drives) == 0:
    return

  # get diary date from first video
  first_fn_mp4 = drives[0][0]
  info = hps.read_video_info(first_fn_mp4)

  creation_ts = info["creation_timestamp"]
  diary_date = datetime.fromtimestamp(creation_ts)
  date = diary_date.strftime("%d:%m:%Y")

  # make diary
  diary = gen.add_diary(date, vehicle)

  # make directorys for drives
  vehicle_id = gen.get_diary()["vehicle_id"]
  diary_token = gen.get_diary()["diary_token"]

  dd_str = diary_date.strftime("%Y-%m-%d")

  l1_path = out_path + "/" + dd_str + "_" + diary_token
  l2_path = l1_path + "/" + "vehicle_" + vehicle_id
  l3_path = l2_path + "/" + "drives"

  os.mkdir(l1_path)
  os.mkdir(l2_path)
  os.mkdir(l3_path)

  gen.save_diary_avro(l2_path + "/drive_diary.avro")

  # go through all videos and xml files and generate drive for each
  for vid_path, xml_path in drives:
    logger.info("Adding drive: ".format(vid_path))
    transform_drive(gen, vid_path, xml_path, l3_path)

  logger.info("--- oneplus transform completed --- ")
  logger.info("Converted schema in: {}".format(out_path))

  logger.info('Now run : data-catalogue diary -t {}'
              ' push -r dump -s {}'.format(diary_token, out_path))
