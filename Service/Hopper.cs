using System.Collections.Concurrent;
using Confluent.Kafka;

namespace Service;

public class Hopper
{
  private readonly IProducer<string, string> _producer;

  private readonly ConcurrentDictionary<string, Action<string>> _onDelivered = new ConcurrentDictionary<string, Action<string>>();

  private readonly ConcurrentDictionary<string, Message<string, string>> _messages = new ConcurrentDictionary<string, Message<string, string>>();

  private readonly ConcurrentDictionary<string, int> _staged = new ConcurrentDictionary<string, int>();

  public string Topic { get; private init; }

  public bool IsActive { get; private set; }

  public int Size => _staged.Count;

  public Hopper(
    IProducer<string, string> producer,
    string topic
  )
  {
    _producer = producer;

    Topic = topic;
  }

  public void Stage(string contextKey, Message<string, string> message, Action<string> onDelivered, int attempt = 1)
  {
    _onDelivered.TryAdd(contextKey, onDelivered);
    _messages.AddOrUpdate(contextKey, _ => message, (_, _) => message);
    _staged.AddOrUpdate(contextKey, _ => attempt, (_, _) => attempt);
  }

  public void Activate()
  {
    if (IsActive)
    {
      return;
    }

    IsActive = true;

    while (IsActive)
    {
      var item = _staged.OrderByDescending(x => x.Value).FirstOrDefault();

      if (
        item.Key != null &&
        _onDelivered.TryRemove(item.Key, out var onDelivered) &&
        _messages.TryRemove(item.Key, out var message) &&
        _staged.TryRemove(item.Key, out var attempt)
      )
      {
        Process(item.Key, message, onDelivered, attempt);
      }
      else
      {
        Thread.Sleep(100);
      }
    }
  }

  public void Pause()
  {
    IsActive = false;
  }

  private void SleepThenRetry(string key, Message<string, string> message, Action<string> onDelivered, int attempt, Error error)
  {
    Console.WriteLine($"Failed: {key}, attempt {attempt}, {error}");

    Thread.Sleep((attempt ^ 2) * 100);

    this.Stage(key, message, onDelivered, attempt + 1);
  }

  private void Process(string key, Message<string, string> message, Action<string> onDelivered, int attempt)
  {
    try
    {
      Console.WriteLine($"Producing {key}, attempt {attempt}");

      _producer.Produce(Topic, message, x =>
      {
        if (x.Error.Code == ErrorCode.NoError)
        {
          onDelivered(key);
        }
        else
        {
          this.SleepThenRetry(key, message, onDelivered, attempt, x.Error);
        }
      });
    }
    catch (KafkaException ex) when (!ex.Error.IsFatal)
    {
      this.SleepThenRetry(key, message, onDelivered, attempt, ex.Error);
    }
  }
}
