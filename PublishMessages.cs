using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQ_ConsistentHashTest
{
    public class PublishMessages
    {
        private List<string> _devices;
        private IConnection _connection;
        public PublishMessages(List<string> devices, IConnection connection)
        {
            _devices = new List<string>();
            _devices.AddRange(devices);
            _connection = connection;
        }


        public void Publish()
        {
            var channel = _connection.CreateModel();
            channel.ExchangeDeclare("exchange-in", ExchangeType.Topic, durable: true, autoDelete: false);
                foreach(var device in _devices)
                {
                    var message = new Message { DeviceId = device };
                    var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));

                    var properties = channel.CreateBasicProperties();
                    properties.Timestamp = new AmqpTimestamp(new DateTimeOffset(DateTime.UtcNow, TimeSpan.Zero).ToUnixTimeSeconds());

                    channel.BasicPublish(
                        exchange: "exchange-in",
                        routingKey: $"deviceid.{device}",
                        basicProperties: properties,
                        body: body);
                }
        }
    }
}
