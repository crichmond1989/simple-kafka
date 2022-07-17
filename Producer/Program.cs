using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Text;
using Confluent.Kafka;
using Service;

var config = new ProducerConfig()
{
  BootstrapServers = "localhost:9092"
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var tasks = new List<Task>();

var builder = new StringBuilder();

foreach (var _ in Enumerable.Range(0, 10_000))
{
  builder.Append("_10B_");
}

var value = builder.ToString();

// NOTE: should be 100 KB
Console.WriteLine($"{Encoding.UTF8.GetByteCount(value)} bytes");

var count = 10_000;

var hopper = new Hopper(producer, "test");

Enumerable.Range(0, count).AsParallel().ForAll(x => hopper.Stage(x.ToString(), value, y => Console.WriteLine($"Delivered: {y}")));

Task.Run(() => hopper.Activate());

while (hopper.Size > 0)
{
  Console.WriteLine($"Remaining: {hopper.Size}");

  Thread.Sleep(5_000);
}

producer.Flush(TimeSpan.FromHours(1));

Console.WriteLine("All Done!!");