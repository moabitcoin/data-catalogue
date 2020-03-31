# Data Catalogue

Tools to run ops on drive datasets.
For proficiency you must be familiar with our drive data schema and [nomenclature](https://gitlab.mobilityservices.io/am/roam/perception/data-catalogue/wikis/Drive-Data-Storage-Schema).
Tools are packaged as a CLI which can be [installed](https://gitlab.mobilityservices.io/am/roam/perception/data-catalogue#installation) and standalone Python [scripts](https://gitlab.mobilityservices.io/am/roam/perception/data-catalogue#3rd_party-dataset-catalogue-tools) for 3rd party data.
Tools contained in this repo ~~`are`~~ will be your one-stop needs for querying our postgres drive database for statistics aggregation, building training sets, setting up annotation projects.
For `3rd_party datasets` you can pull dataset chunks and store them for development.

## Cloning

To fetch this repo and its submodules clone as

```
git clone ssh://git@ssh.gitlab.mobilityservices.io:443/am/roam/perception/data-catalogue.git --recurse-submodules
```

## AWS credentials

The IO ops are performed on S3 and Postgres database hence you'd need your AWS credentials under `~/.aws/credentials`. To setup your `perception` credentials for the first time follow these [steps](https://gitlab.mobilityservices.io/am/roam/perception/team-space/cv-ml-resources/wikis/Onboarding-Perception#your-aws-account). You can configure each profile f.ex

```
# default profile
aws cli configure --profile perception
```

Update your credentials list by adding the following [profiles](https://gitlab.mobilityservices.io/am/roam/perception/drive-data-pipeline/dms-das-perception-drive-data-processor/wikis/Environment-configuration#s3-access).
Please name the profile names as :point_down: for sane CLI usage.
Use the AWS keys provide [here](https://gitlab.mobilityservices.io/am/roam/perception/drive-data-pipeline/dms-das-perception-drive-data-processor/wikis/Environment-configuration#s3-access)

```
# end2end integration testing
das-testing -> aws configure --profile perception-cli-testing
# development break everything
das-data-catalogue-dev -> aws configure --profile perception-cli-develop
# staging with clean valid and models
das-data-catalogue -> aws configure --profile perception-cli-staging
```

## Alpha Fleet Data Catalogue Tools

### Installation

We provide a convenience Makefile to build and run self-contained reproducible Docker images

```
make
make run datadir=/nas
```

The `datadir=/nas` option will mount the host's `/nas` directory to `/data` inside the container.


### Setting up environment

Set up you enviroment before you push/transform data to our buckets. Chose from `default`, `develop`, `testing`, `staging`.

```
# data-catalogue env set <enviroment name>
# To set up develop environment
data-catalogue env set develop
# Check is set up correctly
data-catalogue env get
```

```
[configure.py][get_environ:69][INFO] Environment name : develop
[configure.py][get_environ:70][INFO] Profile name : perception-cli-develop
```

### Usage

See the beautiful help pages we automagically create

```
./bin/data-catalogue --help
```

### Transform, Validate & push

1. [Waylens instructions](https://gitlab.mobilityservices.io/am/roam/perception/data-catalogue/wikis/Perception-Datasets#waylens-drives)
2. [OnePlus instructions](https://gitlab.mobilityservices.io/am/roam/perception/data-catalogue/wikis/Perception-Datasets#oneplus6t-drives)
3. HW Kit instruction


## 3rd_party Data Catalogue Tools

Following scripts would help you fetch/pull 3rd-party dataset from the web/S3.

#### Berkely [Deep-Drive](https://bdd-data.berkeley.edu) Dataset

The following command would download the chunks of dataset in zips at the destination location. The command below would download the zips from the BDD100k website.

```
# http://dl.yf.io/bdd-data/bdd100k/video_parts/bdd100k_videos_train_00.zip
# http://dl.yf.io/bdd-data/bdd100k/video_parts/bdd100k_videos_train_01.zip
# http://dl.yf.io/bdd-data/bdd100k/video_parts/bdd100k_videos_train_02.zip

python scripts/fetch-bdd100k-data.py -c train -r 0 3 -d /data/datasets/bdd/zips/
```


#### myTaxi GPS traces

To fetch myTaxi GPS traces you'd first need to setup your `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. The access keys for myTaxi data give you access to buckets with GPS traces. Ask your realtime team member to provide you with one :).

```
python scripts/fetch-mytaxi-light.py --help
Fetch myTaxi dataset

optional arguments:
  -h, --help            show this help message and exit
  -bucket_name BUCKET_NAME, -b BUCKET_NAME
                        S3 Bucket with objects to download
  -object_name OBJECT_NAME, -o OBJECT_NAME
                        S3 Object in the bucket
  -destination DESTINATION, -d DESTINATION
                        Location where to download the file
```

```
python scripts/fetch-mytaxi-light.py -b s3://com.mytaxi.sophie.das.exchange/booking_location_das_2 -o 00000_0 -d /data/datasets/mytaxi/
```

#### [OpenImagsV5](https://storage.googleapis.com/openimages/web/index.html) Dataset

You can download parts of the dataset by desired label or category.

```
Usage example: python3 downloadOI.py --classes 'vehicle_registration_plate, person' --mode train
```

## List of Datasets
- [Berkley Deep Drive dataset](https://bdd-data.berkeley.edu)
- myTaxi GPS traces `[TODO] : Add link to wiki`
- Alpha Fleet Drive data `[TODO] : Add link to wiki`
- [OpenImagesV5](https://storage.googleapis.com/openimages)

## TODO
- ~~Add S3/url location for data sources~~ [04/22/2019]
-  ~~Mock drive data from BDD100K sample videos~~ [04/30/2019]
-  ~~Mock map updates from BDD100K sample videos~~ [05/02/2019]
-  ~~Oneplus Transform~~ [07/16/2019]

- CLI - WIP : 29/05/2019 : Harsimrat
