import yaml
import argparse
import logging

logging.basicConfig(filename='update_catalogue.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)


def update_catalogue(dataset_name, dataset_info, catalogue_file):

  dataset_name = ' '.join(dataset_name)

  if len(dataset_info) % 2 != 0:
    logging.error('Information key, value pairs don\'t add up')
    return

  info = dict(zip(dataset_info[::2], dataset_info[1::2]))
  logging.info(info)

  try:
    with open(catalogue_file, 'r') as pfile:
      catalogue = yaml.load(pfile)
      catalogue = {} if catalogue is None else catalogue

    if catalogue.get(dataset_name) is not None:
      logging.warning('Datset info will be over-written')
    else:
      catalogue[dataset_name] = {}

    for k, v in info.items():
      catalogue[dataset_name][k] = v

    with open(catalogue_file, 'w') as pfile:
      yaml.dump(catalogue, pfile, default_flow_style=False)

    logging.info('Done writing {}'.format(catalogue_file))

  except Exception as err:
    logging.error('Error updating catalogue file %s, %s', catalogue_file,
                  err)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Script to update catalogue')
    parser.add_argument('-dataset_name', '-d', type=str, nargs='+',
                        help='Dataset Name')
    parser.add_argument('-dataset_info', '-i', type=str, nargs='+',
                        help='Dataset meta data url/count/type')
    parser.add_argument('-catalogue', '-c', type=str,
                        help='Catalogue yaml file')

    args = parser.parse_args()

    update_catalogue(args.dataset_name, args.dataset_info, args.catalogue)
