kafka-topics --bootstrap-server localhost:9092 --create --partitions 15 --replication-factor 1 --topic test --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.1 --if-not-exists