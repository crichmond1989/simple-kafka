using System.Text;
using Confluent.Kafka;
using Service;

var config = new ProducerConfig()
{
  BootstrapServers = "localhost:9092",
  CompressionType = CompressionType.Gzip,
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var tasks = new List<Task>();

var builder = new StringBuilder();

foreach (var _ in Enumerable.Range(0, 10_000))
{
  builder.Append("_10_BYTES_");
}

var value = builder.ToString();

// NOTE: should be 100 KB
Console.WriteLine($"{Encoding.UTF8.GetByteCount(value)} bytes");

var count = 10_000;

var hopper = new Hopper(producer, "test");

Enumerable.Range(0, count).AsParallel().ForAll(x => hopper.Stage(
  x.ToString(),
  new Message<string, string>()
  {
    Key = x.ToString(),
    Value = value,
  },
  y => Console.WriteLine($"Delivered: {y}")
));

Task.Run(() => hopper.Activate());

while (hopper.PendingCount + hopper.StagedCount > 0)
{
  Console.WriteLine($"Remaining Pending: {hopper.PendingCount}");
  Console.WriteLine($"Remaining Staged: {hopper.StagedCount}");

  Thread.Sleep(5_000);
}

producer.Flush(TimeSpan.FromHours(1));

Console.WriteLine("All Done!!");
