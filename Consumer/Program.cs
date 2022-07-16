using Confluent.Kafka;

var config = new ConsumerConfig()
{
  AutoOffsetReset = AutoOffsetReset.Earliest,
  BootstrapServers = "localhost:9092",
  EnableAutoOffsetStore = false,
  GroupId = "crichmond",
  PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();

consumer.Subscribe("test");

while (true)
{
  var result = consumer.Consume(TimeSpan.FromMilliseconds(100));

  if (result == null)
  {
    continue;
  }

  try
  {
    Console.WriteLine($"Consumed: partition {result.Partition.Value}, offset {result.Offset.Value}");

    consumer.StoreOffset(result);
  }
  catch (KafkaException ex) when (!ex.Error.IsFatal)
  {
    Console.WriteLine("Non-fatal exception: {ex}");
  }
}
