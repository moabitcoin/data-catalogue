import argparse

from fleet.s3_ops.diary import Diary
from fleet.s3_ops.drive import Drive
from fleet.s3_ops.sequence import Sequence
from fleet.s3_ops.configure import Configure
from fleet.drive_ops.transform import Transform


def drive_data():

  envs = Configure()
  diary = Diary()
  drive = Drive()
  sequence = Sequence()
  transform = Transform()

  Formatter = argparse.ArgumentDefaultsHelpFormatter

  parser = argparse.ArgumentParser(description='CLI for ops on Drive Data \
                                   (fetch/push/transform)  \
                                   and PostGIS (queries).')
  subparsers = parser.add_subparsers(title='Commands', dest='command',
                                     description='Valid command for CLI',
                                     help='Select a drive data catalogue \
                                     command to run')
  subparsers.required = True

  env_parser = subparsers.add_parser('env',
                                     help='🌍 Configure environment',
                                     formatter_class=Formatter)

  diary_parser = subparsers.add_parser('diary',
                                       help='📓 Diary ops on database',
                                       formatter_class=Formatter)
  drive_parser = subparsers.add_parser('drive',
                                       help='🚘 Drive ops on database',
                                       formatter_class=Formatter)
  sequence_parser = subparsers.add_parser('sequence',
                                          help='🔗 Sequence ops on database',
                                          formatter_class=Formatter)
  transform_parser = subparsers.add_parser('transform',
                                           help='🔀 Transformation ops on drive data',
                                           formatter_class=Formatter)
  envs.build_parser(env_parser)
  drive.build_parser(drive_parser)
  diary.build_parser(diary_parser)
  sequence.build_parser(sequence_parser)
  transform.build_parser(transform_parser)

  args = parser.parse_args()
  args.main(args)
