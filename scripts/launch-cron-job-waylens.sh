# Drive data source directory
SOURCE=$1
# Drive data post transformation
DEST=$2
# Location of your data-catalogue cli github repo
CLI_DIR=$3
# which enviroment you configured to run the cli
CONDA_ENV=$4

echo cd $CLI_DIR
cd $CLI_DIR
echo conda activate $CONDA_ENV
conda activate $CONDA_ENV

# Cron command
echo python scripts/waylens-transform-and-push.py -s $SOURCE -d $DEST
python scripts/waylens-transform-and-push.py -s $SOURCE -d $DEST
