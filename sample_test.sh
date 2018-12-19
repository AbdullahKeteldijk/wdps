# if [ -z "${SPARK_HOME}" ]; then
#   export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
# fi

# # Load the Spark configuration
# . "${SPARK_HOME}/sbin/spark-config.sh"

# # Start Master
# "${SPARK_HOME}/sbin"/start-master.sh

# # Start Workers
# "${SPARK_HOME}/sbin"/start-slaves.sh

SCRIPT=${1:-"EntityRecognition5.py"}
INFILE=${2:-"/home/wdps1813/scratch/wdps1813/wdps/data/sample.warc.gz"}
OUTFILE=${3:-"folder"}


PYSPARK_PYTHON=$(readlink -f $(which python)) ~/local/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
--master yarn $SCRIPT $INFILE $OUTFILE

hdfs dfs -cat $OUTFILE"/*" > $OUTFILE
