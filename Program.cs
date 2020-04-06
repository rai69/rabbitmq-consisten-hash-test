using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RabbitMQ_ConsistentHashTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var deviceIds = System.IO.File.ReadAllLines($".\\devices.txt");
            Console.WriteLine($"{deviceIds.Length}");

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                var tasklist = new List<Task>();
                var comsumerList = new List<Consumer>();
                for (var consumerId = 0; consumerId < 5; consumerId++)
                {
                    var pgw = new Consumer(consumerId, connection);
                    tasklist.Add(pgw.RunAsync());
                    comsumerList.Add(pgw);
                }


                var pm = new PublishMessages(deviceIds.ToList(), connection);
                pm.Publish();

                Console.WriteLine("Done - Press enter to stop");
                Console.ReadLine();
            }
        }
    }
}
