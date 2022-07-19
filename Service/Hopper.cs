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

    // NOTE: Due to concurrency, Decrement/Increment needs to be a locked operation. Under the hood,
    //       the typical _stagedCount++ syntax does three different operations: READ, ADD, SAVE.
    //       Another thread could call SAVE inbetween this thread's READ -> ADD, or ADD -> SAVE.
    //       This would cause a missed increment.
    Interlocked.Increment(ref _stagedCount);

    // NOTE: This operation is similar to TryAdd, but it's handy because we're more focused on
    //       retrieving the value versus setting the value. It's also nice to not have to worry about
    //       the `var queue` being null. I've seen this implemented as an extension method
    //       regardless of concurrency.
    var queue = _priorityQueue.GetOrAdd(attempt, _ => new ConcurrentQueue<string>());

    // NOTE: TryAdd returns false if the key already exists. This could occur in our workflow if one
    //       of the consumers was assigned to Partition X, but then a partition reassignment occured,
    //       causing another consumer to be assigned Partition X.
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
      // NOTE: This is the most effective backoff strategy. There's no need to add more items to
      //       the queue if the queue is full.
      while (_lastDelivery < _lastRejection && DateTime.UtcNow.AddSeconds(-10).Ticks < _lastRejection)
      {
        // NOTE: While we're waiting for the queue to empty, we may as well put priority on
        //       processing delivery reports.
        _producer.Poll(TimeSpan.FromMilliseconds(100));
      }

      // NOTE: Prioritize retries, so grab the highest attempt queue that has any items
      var (attempt, queue) = _priorityQueue.OrderByDescending(x => x.Key).FirstOrDefault(x => x.Value.Any());

      // NOTE: Due to concurrency, it's possible that an entity was processed on another thread.
      //       If either TryRemove returns false, then the entity was processed, and we only
      //       need to process at least once.
      //       The TryDequeue will return false when it is empty. If empty, then we Sleep to avoid
      //       unnecessary processing while we're waiting for more items.
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

  private void Retry(string key, Message<string, string> message, Action<string> onDelivered, int attempt, Error error)
  {
    Console.WriteLine($"Failed: {key}, attempt {attempt}, {error}");

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
          this.Retry(key, message, onDelivered, attempt, x.Error);
        }
      });

      Interlocked.Increment(ref _pendingCount);
    }
    catch (KafkaException ex) when (!ex.Error.IsFatal)
    {
      _lastRejection = DateTime.UtcNow.Ticks;

      this.Retry(key, message, onDelivered, attempt, ex.Error);
    }
  }
}
