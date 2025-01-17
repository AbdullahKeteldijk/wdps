ES_PORT=9200
ES_BIN=$(realpath ~/scratch/elasticsearch-2.4.1/bin/elasticsearch)

>.es_log*
prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
until [ -n "$(cat .es_log* | grep YELLOW)" ]; do sleep 1; done
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

python3 popularity.py $ES_NODE:$ES_PORT "Vrije Universiteit Amsterdam"

kill $ES_PID

### OLD test_elasticsearch.sh ###
# ES_PORT=9200
# ES_BIN=/home/bbkruit/scratch/wdps/elasticsearch-2.4.1/bin/elasticsearch

# prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
# echo "waiting 15 seconds for elasticsearch to set up..."
# sleep 15
# ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)')
# ES_PID=$!
# echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"

# python3 elasticsearch.py $ES_NODE:$ES_PORT "Obama"

# kill $ES_PID
