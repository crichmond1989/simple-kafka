using System.Collections.Concurrent;
using Confluent.Kafka;

namespace Service;

public class Hopper
{
  private readonly IProducer<string, string> _producer;

  private readonly ConcurrentDictionary<string, Action<string>> _onDelivered = new();

  private readonly ConcurrentDictionary<string, Message<string, string>> _messages = new();

  private readonly ConcurrentDictionary<int, ConcurrentQueue<string>> _priorityQueue = new();

  private int _pendingCount;

  private int _stagedCount;

  private long _lastDelivery;

  private long _lastRejection;

  public bool IsActive { get; private set; }

  public string Topic { get; private init; }

  public int PendingCount => _pendingCount;

  public int StagedCount => _stagedCount;

  public Hopper(
    IProducer<string, string> producer,
    string topic
  )
  {
    _producer = producer;

    Topic = topic;
  }

  public void Stage(string key, Message<string, string> message, Action<string> onDelivered, int attempt = 1)
  {
    Console.WriteLine($"Staged: {key}");

    Interlocked.Increment(ref _stagedCount);

    var queue = _priorityQueue.GetOrAdd(attempt, _ => new ConcurrentQueue<string>());

    _onDelivered.TryAdd(key, onDelivered);
    _messages.TryAdd(key, message);

    queue.Enqueue(key);
  }

  public void Activate()
  {
    if (IsActive)
    {
      // NOTE: Already active, no need to spin up another loop
      return;
    }

    IsActive = true;

    while (IsActive)
    {
      while (_lastDelivery < _lastRejection)
      {
        _producer.Poll(TimeSpan.FromMilliseconds(100));
      }

      var (attempt, queue) = _priorityQueue.OrderByDescending(x => x.Key).FirstOrDefault(x => x.Value.Count > 0);

      if (
        queue?.TryDequeue(out var key) == true &&
        _onDelivered.TryRemove(key, out var onDelivered) &&
        _messages.TryRemove(key, out var message)
      )
      {
        Process(key, message, onDelivered, attempt);
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
    Interlocked.Decrement(ref _stagedCount);

    try
    {
      Console.WriteLine($"Producing: {key}, attempt {attempt}");

      _producer.Produce(Topic, message, x =>
      {
        Interlocked.Decrement(ref _pendingCount);

        if (x.Error.Code == ErrorCode.NoError)
        {
          Console.WriteLine($"Delivered: {key}");

          _lastDelivery = DateTime.UtcNow.Ticks;

          onDelivered(key);
        }
        else
        {
          Task.Run(() => this.SleepThenRetry(key, message, onDelivered, attempt, x.Error));
        }
      });

      Interlocked.Increment(ref _pendingCount);
    }
    catch (KafkaException ex) when (!ex.Error.IsFatal)
    {
      _lastRejection = DateTime.UtcNow.Ticks;

      Task.Run(() => this.SleepThenRetry(key, message, onDelivered, attempt, ex.Error));
    }
  }
}
