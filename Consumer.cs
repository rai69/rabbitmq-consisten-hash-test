using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ_ConsistentHashTest
{
    public class Consumer
    {
        private readonly int _instanceId;
        private readonly IConnection _connection;
        private List<string> _devices;
        public Consumer(int instanceId, IConnection connection)
        {
            _instanceId = instanceId;
            _connection = connection;
            _devices = new List<string>();
        }
        public Task RunAsync()
        {
            using (var channel = _connection.CreateModel())
            {
                channel.ExchangeDeclare("exchange-in", ExchangeType.Topic, durable: true, autoDelete: false);
                channel.ExchangeDeclare("exchange-consumer", "x-consistent-hash", durable: true, autoDelete: false);
                channel.ExchangeBind(
                    destination: "exchange-consumer",
                    source: "exchange-in",
                    routingKey: "deviceid.*");

                var queueName = $"consumer.{_instanceId}";
                var queue = channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false);
                channel.QueueBind(queueName, "exchange-consumer", "1");

                // Start consuming the queue and delegate
                // messages to `OnNewControlMessage`
                var consumer = new EventingBasicConsumer(channel);

                // Consuming is skipped 
                // consumer.Received += OnReceive;

                channel.BasicConsume(
                    queue: queueName,
                    autoAck: false,
                    consumer: consumer);

                return Task.FromResult(0);
            }
        }

        private void OnReceive(object sender, BasicDeliverEventArgs e)
        {
           var consumer = (EventingBasicConsumer)sender;

            try
            {
                var message = ParseRabbitMqMessageToMessage(e);

                _devices.Add(message.DeviceId);
                
                System.IO.File.WriteAllLines($".\\devices-{_instanceId}.txt", _devices);

                consumer.Model.BasicAck(e.DeliveryTag, false);
            }        
            catch (Exception ex)
            {
                Console.WriteLine($"Consumer: {_instanceId}: {ex.Message}");
                consumer.Model.BasicNack(e.DeliveryTag, false, requeue: false);
            }
        }

        private Message ParseRabbitMqMessageToMessage(BasicDeliverEventArgs args)
        {
            var message = Encoding.UTF8.GetString(args.Body);

            using (var stringReader = new StringReader(message))
            {
                var content = stringReader.ReadToEnd();
                return JsonConvert.DeserializeObject<Message>(content);
            }
           
        }
    }
}
