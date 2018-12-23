### Starting elasticsearch ###
ES_PORT=9200
ES_BIN=$(realpath ~/scratch/elasticsearch-2.4.1/bin/elasticsearch)

>.es_log*
prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

### Starting Trident ###
KB_PORT=9090
KB_BIN=/home/bbkruit/scratch/trident/build/trident
KB_PATH=/home/jurbani/data/motherkb-trident

prun -o .kb_log -v -np 1 $KB_BIN server -i $KB_PATH --port $KB_PORT </dev/null 2> .kb_node &
echo "waiting 5 seconds for trident to set up..."
until [ -n "$KB_NODE" ]; do KB_NODE=$(cat .kb_node | grep '^:' | grep -oP '(node...)') ; done
sleep 5
KB_PID=$!
echo "trident should be running now on node $KB_NODE:$KB_PORT and $KB_NODE2:$KB_PORT   (connected to process $KB_PID)"


time python3 EntityRecognition.py 

python3 UrbanDatabase.py

python3 CSV_merge.py

python3 EntityLinking.py $KB_NODE:$KB_PORT $ES_NODE:$ES_PORT > sample_predictions.tsv

python3 score.py data/sample.annotations.tsv sample_predictions.tsv


