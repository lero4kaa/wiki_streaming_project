```
bash run-cluster.sh
docker exec cassandra-node1 cqlsh -it
docker exec cassandra-node1 cqlsh -e "USE wiki_project; DESCRIBE TABLES;"
docker exec cassandra-node1 cqlsh -e "USE wiki_project; SELECT * FROM ;"
bash shutdown-cluster.sh
```