
using System.Collections.Concurrent;

namespace KafkaTest
{
    internal class Engine
    {
        ConcurrentQueue<long> _offsets = new ConcurrentQueue<long>();
        public void Main()
        {
            CalculateMetric();
            var kafka = new KafkaService();
            kafka.OnMessageArrived += Kafka_OnMessageArrived;
            Task.Run(() =>
            {
                while (true)
                {
                    //kafka.Produce("general-super-star", "my-key", DateTime.Now.Ticks.ToString());
                    kafka.ProduceAsync("general-super-star", "my-key", DateTime.Now.Ticks.ToString());
                    //Thread.Sleep(1);
                }
            });

            kafka.Consume("general-super-star");
        }

        private void Kafka_OnMessageArrived(object? sender, OnGetMessageArgs e)
        {
            Task.Run(() =>
           {
               _offsets.Enqueue(e.Offset);
           });

        }

        private void CalculateMetric()
        {
            Task.Run(() =>
            {
                while (true)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                    Console.WriteLine(_offsets.Count + "per second");
                    _offsets.Clear();
                }
            });
        }
    }
}
