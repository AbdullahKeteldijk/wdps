ES_PORT=9200
ES_BIN=/home/bbkruit/scratch/wdps/elasticsearch-2.4.1/bin/elasticsearch

prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting 15 seconds for elasticsearch to set up..."
sleep 15
ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)')
ES_PID=$!
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"



KB_PORT=9090
KB_BIN=/home/bbkruit/scratch/trident/build/trident
KB_PATH=/home/jurbani/data/motherkb-trident

prun -o .kb_log -v -np 1 $KB_BIN server -i $KB_PATH --port $KB_PORT </dev/null 2> .kb_node &
echo "waiting 5 seconds for trident to set up..."
sleep 5
KB_NODE=$(cat .kb_node | grep '^:' | grep -oP '(node...)')
KB_PID=$!
echo "trident should be running now on node $KB_NODE:$KB_PORT (connected to process $KB_PID)"

# python3 sparql.py $KB_NODE:$KB_PORT "select * where {?s ?p ?o} limit 10"
# python3 sparql.py $KB_NODE:$KB_PORT "select * where {<http://rdf.freebase.com/ns/m.01cx6d_> ?p ?o} limit 100"

# python3 sparql.py $KB_NODE:$KB_PORT "select * where {?s <http://www.w3.org/2002/07/owl#sameAs> <http://rdf.freebase.com/ns/m.0k3p> . ?s <http://www.w3.org/2002/07/owl#sameAs> ?o .}"

python3 SPARQL_code.py $ES_NODE:$ES_PORT, $KB_NODE:$KB_PORT

#kill $KB_PID
#kill $ES_PID
