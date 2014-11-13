using System;
using System.Threading.Tasks;
using EasyNetQ;
using EasyNetQ.Topology;

namespace SampleApp
{
    class Program
    {
        private const string ConsumerExchangeName = "consumer_exchange";
        private const string ConsumerQueueName = "consumer_queue";
        private const string RoutingKey = "key";
        private const string PublisherExchangeName = "publisher_exchange";
        private const string PublisherQueueName = "publisher_queue";

        private static IExchange _publisherExchange;
        private static IExchange _consumerExchange;
        private static IQueue _consumerQueue;


        /// <summary>
        /// Illustrates the delays on publishing
        /// </summary>
        /// <param name="args"></param>

        static void Main(string[] args)
        {
            var consumerBus = RabbitHutch.CreateBus("host=localhost;prefetchCount=50");

            var publisherBus = RabbitHutch.CreateBus("host=localhost;prefetchCount=50");

            DeclareExchangesAndQueues(consumerBus);

            FillConsumerQueue(consumerBus);


            Func<IMessage<TestDto>, MessageReceivedInfo, Task> advancedAction = 
                (msg, receivedInfo) => Task.Factory.StartNew(() =>
                {
                    msg.Body.Text = string.Format("{0}_{1}", msg.Body.Text, "toPublish"); 
                    Publish(publisherBus, msg, _publisherExchange, RoutingKey);
                });

            Subscribe(consumerBus, advancedAction);
        }

        static void FillConsumerQueue(IBus bus)
        {
            for (int i = 0; i < 100; i++)
            {
                var msg = new TestDto {Text = string.Format("Message # {0}", i)};

                Publish(bus, msg, _consumerExchange, RoutingKey);
            }
        }

        static void DeclareExchangesAndQueues(IBus bus)
        {
            _consumerExchange = bus.Advanced.ExchangeDeclare(ConsumerExchangeName, "topic");
            _consumerQueue = bus.Advanced.QueueDeclare(ConsumerQueueName);
            bus.Advanced.Bind(_consumerExchange, _consumerQueue, RoutingKey);


            _publisherExchange = bus.Advanced.ExchangeDeclare(PublisherExchangeName, "topic");
            IQueue queue = bus.Advanced.QueueDeclare(PublisherQueueName);
            bus.Advanced.Bind(_publisherExchange, queue, RoutingKey);
        }

        public static void Subscribe<T>(IBus bus, Func<IMessage<T>, MessageReceivedInfo, Task> action) where T : class
        {
           bus.Advanced.Consume(_consumerQueue, action);
        }

        public static void Publish<T>(IBus bus, T msg, IExchange exchange, string routingKey) where T : class
        {
            try
            {
                var message = new Message<T>(msg);
                message.Properties.DeliveryMode = 2;

                bus.Advanced.Publish(exchange, routingKey, false, false, message);
            }
            catch (EasyNetQException e)
            {
                throw;
            }
        }
    }
}
