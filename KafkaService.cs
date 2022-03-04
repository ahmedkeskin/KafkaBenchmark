using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace KafkaTest
{

    internal class KafkaService
    {

        private readonly string _kafkaServer = "192.168.1.86:9092";
        private readonly string _consumerGroupId = "super-stars";
        private readonly string _clientId = "xanax";

        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        IProducer<string, string> _producer;
        IConsumer<string, string> _consumer;
        public event EventHandler<OnGetMessageArgs> OnMessageArrived;

        private long _duration = 0;
        private long _offset = 0;
        private long _offsetDiff = 0;

        public KafkaService()
        {
            _producerConfig = new ProducerConfig()
            {
                BootstrapServers = _kafkaServer,
                ClientId = _clientId

            };
            _producer = new ProducerBuilder<string, string>(_producerConfig).Build();

            _consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = _kafkaServer,
                GroupId = _consumerGroupId,
                ClientId = _clientId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();
        }
        private Message<string, string> PopulateMessage<T>(string messageKey, T messageObject)
        {
            try
            {
                var messageString = JsonSerializer.Serialize<T>(messageObject);
                return new Message<string, string>() { Key = null, Value = messageString };
            }
            catch (Exception exception)
            {
                throw;
            }
        }
        public void Produce<T>(string topic, string messageKey, T messageObject)
        {
            try
            {
                _producer.Produce(topic, PopulateMessage(messageKey, messageObject));
                _producer.Flush(TimeSpan.FromSeconds(3));
            }
            catch (Exception exception)
            {

                throw;
            }
        }
        public void ProduceAsync<T>(string topic, string messageKey, T messageObject)
        {
            try
            {
                _producer.ProduceAsync(topic, PopulateMessage(messageKey, messageObject));
             //   _producer.Flush(TimeSpan.FromSeconds(3));
            }
            catch (Exception exception)
            {

                throw;
            }
        }
        public void Consume(string topic)
        {
            _consumer.Subscribe(topic);
            var stopwatch = new Stopwatch();
            var stopwatch2 = new Stopwatch();

            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;
            EventHandler<OnGetMessageArgs> handler = OnMessageArrived;
            if (handler != null)
            {
                while (true)
                {
                    var consumeResult = _consumer.Consume(token);
                    var consumeEventArg = new OnGetMessageArgs() { Offset = consumeResult.Offset.Value};   
                    handler.Invoke(this, consumeEventArg);
                }
            }
        }

        public void ConsumeAsync(string topic)
        {

        }




    }
    public class OnGetMessageArgs : EventArgs
    {
        public long Offset { get; set; }
    }
}
