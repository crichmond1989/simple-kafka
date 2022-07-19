using Confluent.Kafka;

var config = new ConsumerConfig()
{
  AutoOffsetReset = AutoOffsetReset.Earliest,
  BootstrapServers = "localhost:9092",
  ClientId = "Consumer",
  EnableAutoOffsetStore = false,
  GroupId = "crichmond",
  PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
};

var threads = Environment.ProcessorCount;

if (threads < 1)
{
  threads = 1;
}

Parallel.ForEach(new byte[threads], _ =>
{
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
      Console.WriteLine($"Non-fatal exception: partition {result.Partition.Value}, offset {result.Offset.Value}, {ex}");
    }
  }
});