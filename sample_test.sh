SCRIPT=${1:-"EntityRecognition6.py"}

# This assumes there is a python virtual environment in the "venv" directory
source venv/bin/activate
virtualenv --relocatable venv
zip -r venv.zip venv

PYSPARK_PYTHON=$(readlink -f python3) ~/../../local/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./VENV/venv/bin/python3 \
--executor-memory 5g \
--num-executors 20 \
--conf spark.memory.fraction=0.8 \
--conf spark.yarn.am.memory=6g \
--archives venv.zip#VENV \
--master yarn --deploy-mode cluster \
$SCRIPT





# # Load the Spark configuration
# . "${SPARK_HOME}/sbin/spark-config.sh"

# # Start Master
# "${SPARK_HOME}/sbin"/start-master.sh

# # Start Workers
# "${SPARK_HOME}/sbin"/start-slaves.sh

# SCRIPT=${1:-"EntityRecognition5.py"}
# INFILE=${2:-"hdfs:///user/bbkruit/CommonCrawl-sample.warc.gz"}
# STANFORD=${3:-"/home/wdps1813/scratch/wdps1813/wdps/stanford-ner-2017-06-09"}
# OUTFILE=${4:-"folder"}


# PYSPARK_PYTHON=$(readlink -f $(which python)) ~/../../local/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
# --master yarn $SCRIPT #$INFILE #$STANFORD $OUTFILE

# hdfs dfs -cat $OUTFILE"/*" > $OUTFILE
# time python3 EntityRecognition5.py <(hdfs dfs -cat hdfs:///user/bbkruit/sample.warc.gz | zcat)

# ~/../../local/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --class Spark.SparkScript --executor-memory 5g --num-executors 20 --conf spark.memory.fraction=0.8 --conf spark.yarn.am.memory=6g --master yarn --master yarn-cluster --executor-memory 5g --num-executors 20

# PYSPARK_PYTHON=$(readlink -f $(python)) ~/../../local/spark/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
# --executor-memory 5g \
# --num-executors 20 \
# --conf spark.memory.fraction=0.8 \
# --conf spark.yarn.am.memory=6g \
# --master yarn-cluster \ 
# $SCRIPT $INFILE


# time python3 EntityRecognition5.py <(hdfs dfs -cat hdfs:///user/bbkruit/sample.warc.gz | zcat) 

