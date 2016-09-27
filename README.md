# KafkaConnect
A simplified clone of [kafka-connect-hdfs](https://github.com/confluentinc/kafka-connect-hdfs) which uses Protobuf  instead of Avro

```
curl -H "Content-Type: application/json" -X POST -d '{"name": "hdfs-sink-connector","config": {"connector.class": "org.github.etacassiopeia.kafka.connect.hdfs.HdfsSinkConnector","format.class": "org.github.etacassiopeia.kafka.connect.hdfs.parquet.ParquetFormat","partitioner.class": "org.github.etacassiopeia.kafka.connect.hdfs.partitioner.ProtocolPartitioner","tasks.max": "3","topics": "normalized","hdfs.url": "hdfs://localhost:8020","hadoop.conf.dir": "/home/mohsen/hadoop/etc/hadoop","hadoop.home": "/home/mohsen/hadoop","flush.size": "100000","rotate.interval.ms": "10000"}}' http://localhost:36912/connectors
```

```
{
    "name": "hdfs-sink-connector",
    "config": {
        "connector.class": "org.github.etacassiopeia.kafka.connect.hdfs.HdfsSinkConnector",
        "format.class": "org.github.etacassiopeia.kafka.connect.hdfs.parquet.ParquetFormat",
        "partitioner.class": "org.github.etacassiopeia.kafka.connect.hdfs.partitioner.ProtocolPartitioner",
        "tasks.max": "3",
        "topics": "test-topic",
        "hdfs.url": "hdfs://localhost:8020",
        "hadoop.conf.dir": "/home/mohsen/hadoop/etc/hadoop",
        "hadoop.home": "/home/mohsen/hadoop",
        "flush.size": "100",
        "rotate.interval.ms": "1000"
    }
}
```
