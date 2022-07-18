using System.Collections.Concurrent;
using Confluent.Kafka;
using Service;

var fromTopic = "test.pipe";
var toTopic = "test.pipe.v2";

var config = new ConsumerConfig()
{
  AutoOffsetReset = AutoOffsetReset.Earliest,
  BootstrapServers = "localhost:9092",
  EnableAutoOffsetStore = false,
  GroupId = "crichmond",
  PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
};

var threads = Environment.ProcessorCount;

if (threads < 1)
{
  threads = 1;
}

var pConfig = new ProducerConfig()
{
  BootstrapServers = "localhost:9092",
  CompressionType = CompressionType.Gzip,
};

using var producer = new ProducerBuilder<string, string>(pConfig).Build();

Parallel.ForEach(new byte[threads], _ =>
{
  var hopper = new Hopper(producer, toTopic);

  Task.Run(() => hopper.Activate()).ContinueWith(t =>
  {
    if (t.Exception != null)
    {
      Console.WriteLine($"Hopper Active exception: {t.Exception.InnerException}");
    }
  });

  var offsetTracker = new ConcurrentDictionary<TopicPartitionOffset, byte>();

  using var consumer = new ConsumerBuilder<string, string>(config).Build();

  consumer.Subscribe(fromTopic);

  void StoreMinDeliveredOffset(TopicPartitionOffset pos)
  {
    var ready = offsetTracker
      .Where(x => x.Key.Topic == pos.Topic)
      .Where(x => x.Key.Partition.Value == pos.Partition.Value)
      .OrderBy(x => x.Key.Offset.Value)
      .TakeWhile(x => x.Value == 1)
      .ToArray();

    foreach (var item in ready)
    {
      offsetTracker.TryRemove(item);
    }

    var last = ready.Last();

    if (last.Value == 0)
    {
      return;
    }

    // NOTE: Commit refers to the message that should be read next
    var commitPos = new TopicPartitionOffset(last.Key.TopicPartition, last.Key.Offset + 1);

    consumer.StoreOffset(commitPos);
  }

  while (true)
  {
    var result = consumer.Consume(TimeSpan.FromMilliseconds(100));

    if (result?.Message == null)
    {
      continue;
    }

    var pos = result.TopicPartitionOffset;

    try
    {
      Console.WriteLine($"Consumed: {pos}");

      if (!offsetTracker.TryAdd(pos, 0))
      {
        StoreMinDeliveredOffset(pos);
        continue;
      }

      var message = new Message<string, string>()
      {
        Key = result.Message.Key,
        Value = result.Message.Value,
      };

      hopper.Stage(pos.ToString(), message, _ =>
      {
        Console.WriteLine($"Track Update: {pos}");

        offsetTracker.TryUpdate(pos, 1, 0);

        StoreMinDeliveredOffset(pos);
      });
    }
    catch (KafkaException ex) when (!ex.Error.IsFatal)
    {
      Console.WriteLine($"Non-fatal exception: partition {result.Partition.Value}, offset {result.Offset.Value}, {ex}");
    }
  }
});

