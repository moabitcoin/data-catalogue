# Author : Sunita Nayak, Big Vision LLC
# Usage: python3 downloadOI.py --classes 'Ice_cream,Cookie' --mode train

import csv
import os
import logging
import argparse
import argparse
import subprocess
from tqdm import tqdm
import multiprocessing
from multiprocessing import Pool as thread_pool

logger = logging.getLogger(__name__)
c_handler = logging.StreamHandler()
c_format = logging.Formatter('[%(name)s][%(levelname)s][%(message)s]')
c_handler.setFormatter(c_format)
logger.addHandler(c_handler)


cpu_count = multiprocessing.cpu_count()

parser = argparse.ArgumentParser(description="Download Class specific "
                                 "images from OpenImagesV5")
parser.add_argument("--mode", help="Dataset category - train"
                    ", validation or test", required=True)
parser.add_argument("--classes", help="Names of object classes"
                    "to be downloaded", required=True)
parser.add_argument("--nthreads", help="Number of threads to use",
                    required=False, type=int, default=cpu_count * 2)
parser.add_argument("--occluded", help="Include occluded images",
                    required=False, action='store_true', default=False)
parser.add_argument("--truncated", help="Include truncated images",
                    required=False, action='store_true', default=False)
parser.add_argument("--groupOf", help="Include groupOf images",
                    required=False, action='store_true', default=False)
parser.add_argument("--depiction", help="Include depiction images",
                    required=False, action='store_true', default=False)
parser.add_argument("--inside", help="Include inside images",
                    required=False, action='store_true', default=False)

args = parser.parse_args()

run_mode = args.mode

threads = args.nthreads

classes = []
for class_name in args.classes.split(','):
    classes.append(class_name)

with open('csvs/class-descriptions-boxable.csv', mode='r') as infile:
    reader = csv.reader(infile)
    dict_list = {rows[1]: rows[0] for rows in reader}

os.makedirs(run_mode, exist_ok=True)

pool = thread_pool(threads)
commands = []
cnt = 0

for ind in range(0, len(classes)):

    class_name = classes[ind]
    logging.info("Class " + str(ind) + " : " + class_name)

    os.makedirs(run_mode + '/' + class_name, exist_ok=True)

    command = "grep " + dict_list[class_name.replace('_', ' ')] + " csvs/" \
        + run_mode + "-annotations-bbox.csv"
    class_annotations = subprocess.run(command.split(),
                                       stdout=subprocess.PIPE).stdout.decode('utf-8')
    class_annotations = class_annotations.splitlines()

    for line in tqdm(class_annotations):

        line_parts = line.split(',')

        # IsOccluded,IsTruncated,IsGroupOf,IsDepiction,IsInside
        if (not args.occluded and int(line_parts[8]) > 0):
            logging.warning("Skipped %s", line_parts[0])
            continue
        if (not args.truncated and int(line_parts[9]) > 0):
            logging.warning("Skipped %s", line_parts[0])
            continue
        if (not args.groupOf and int(line_parts[10]) > 0):
            logging.warning("Skipped %s", line_parts[0])
            continue
        if (not args.depiction and int(line_parts[11]) > 0):
            logging.warning("Skipped %s", line_parts[0])
            continue
        if (not args.inside and int(line_parts[12]) > 0):
            logging.warning("Skipped %s", line_parts[0])
            continue

        cnt = cnt + 1

        command = 'aws s3 --no-sign-request ' + \
            '--only-show-errors cp s3://open-images-dataset/' + \
            run_mode + '/' + line_parts[0] + '.jpg ' + run_mode + \
            '/' + class_name + '/' + line_parts[0] + '.jpg'

        commands.append(command)

        with open('{}/{}/{}.txt'.format(run_mode, class_name, line_parts[0]),
                  'a') as f:
            f.write(','.join([class_name, line_parts[4], line_parts[5],
                              line_parts[6], line_parts[7]]) + '\n')

logging.info("Annotation Count : " + str(cnt))
commands = list(set(commands))
logging.info("Number of images to be downloaded : " + str(len(commands)))

list(tqdm(pool.imap(os.system, commands), total=len(commands)))

pool.close()
pool.join()
