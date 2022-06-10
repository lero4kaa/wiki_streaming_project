docker network create my-cassandra-network
docker run --name cassandra-node1 --network my-cassandra-network -d cassandra:latest
docker run --name cassandra-node2 --network my-cassandra-network -d -e CASSANDRA_SEEDS=cassandra-node1 cassandra:latest
docker run --name cassandra-node3 --network my-cassandra-network -d -e CASSANDRA_SEEDS=cassandra-node1 cassandra:latest

sleep 100

# Copying ddl and dml files into cassandra-node1 container
docker cp ddl.cql cassandra-node1:/
docker cp dml.cql cassandra-node1:/
docker exec cassandra-node1 cqlsh -f ddl.cql
