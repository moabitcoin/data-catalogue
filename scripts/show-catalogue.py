import yaml
import argparse
import logging

logging.basicConfig(filename='show_catalogue.log', filemode='w',
                    format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)


def show_catalogue(dataset_name, catalogue_file):

  dataset_name = ' '.join(dataset_name)

  try:
    with open(catalogue_file, 'r') as pfile:
      catalogue = yaml.load(pfile)
    logging.info(catalogue)

    if catalogue.get(dataset_name) is None:
      logging.info('Dataset %s not found', dataset_name)
    else:
      info = catalogue[dataset_name]
      logging.info(info)

  except Exception as err:
    logging.error('Error showing data catalogue file %s, %s', catalogue_file,
                  err)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Script to update catalogue')
    parser.add_argument('-dataset_name', '-d', type=str, nargs='+',
                        default='', help='Dataset Name')
    parser.add_argument('-catalogue', '-c', type=str,
                        help='Catalogue yaml file')

    args = parser.parse_args()

    show_catalogue(args.dataset_name, args.catalogue)
