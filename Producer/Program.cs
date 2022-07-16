using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Text;
using Confluent.Kafka;

var config = new ProducerConfig()
{
  BootstrapServers = "localhost:9092"
};

using var producer = new ProducerBuilder<string, string>(config).Build();

var tasks = new List<Task>();

var builder = new StringBuilder();

foreach (var _ in Enumerable.Range(0, 20_000))
{
  builder.Append("this is a test value");
}

var value = builder.ToString();

var init = Enumerable.Range(0, 100_000).ToDictionary(x => x.ToString(), x => 1);

var queue = new ConcurrentDictionary<string, int>(init);

var pending = new ConcurrentDictionary<string, int>();

void SleepThenTryAgain(string key, int attempt)
{
  Thread.Sleep((2 ^ attempt) * 100);

  queue!.AddOrUpdate(key, _ => attempt + 1, (_, _) => attempt + 1);
  pending!.TryRemove(new KeyValuePair<string, int>(key, attempt));
}

void TryProduce(string key, int attempt)
{
  var message = new Message<string, string>()
  {
    Key = key,
    Value = value!,
  };

  try
  {
    pending!.AddOrUpdate(key, _ => attempt, (_, _) => attempt);

    Console.WriteLine($"Produce {key}, attempt {attempt}");

    producer!.Produce("test", message, x =>
    {
      if (x.Error.Code == ErrorCode.NoError)
      {
        Console.WriteLine($"Delivered: {key}");

        pending.TryRemove(new KeyValuePair<string, int>(key, attempt));
      }
      else
      {
        Console.WriteLine($"Failed: {key}, attempt {attempt}, error {x.Error}");
        SleepThenTryAgain(key, attempt);
      }
    });
  }
  catch (KafkaException ex)
  {
    if (ex.Error.IsFatal)
    {
      throw;
    }

    Console.WriteLine($"Failed: {key}, attempt {attempt}, error {ex.Error}");
    SleepThenTryAgain(key, attempt);
  }
}

while (queue.Any() || pending.Any())
{
  var item = queue.FirstOrDefault();

  if (item.Key != null)
  {
    queue.TryRemove(item);

    TryProduce(item.Key, item.Value);
  }
  else
  {
    producer.Flush(TimeSpan.FromMilliseconds(100));
  }
}

Console.WriteLine("All done");
