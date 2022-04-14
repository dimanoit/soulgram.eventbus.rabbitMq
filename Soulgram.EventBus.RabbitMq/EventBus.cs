using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Soulgram.Eventbus;
using Soulgram.Eventbus.Interfaces;

namespace Soulgram.EventBus.RabbitMq
{
    // TODO add logging and exception handling
    public class EventBus : IEventBus, IDisposable
    {
        private readonly IRabbitMQConnection _persistentConnection;
        private readonly IEventBusSubscriptionsManager _subsManager;
        private readonly IServiceProvider _serviceProvider;

        private IModel _consumerChannel;
        private string _queueName;
        private string _exchange;

        public EventBus(
            IRabbitMQConnection persistentConnection,
            IEventBusSubscriptionsManager subsManager,
            IServiceProvider serviceProvider,
            string exchange,
            string queueName = null)
        {
            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            _exchange = exchange ?? throw new ArgumentNullException(nameof(exchange));

            _subsManager = subsManager ?? new InMemoryEventBusSubscriptionsManager();

            _queueName = queueName;
            _consumerChannel = CreateConsumerChannel();
            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;
        }

        public void Publish(IntegrationEvent @event)
        {
	        var eventName = @event.GetType().Name;

	        var body = JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), new JsonSerializerOptions
	        {
		        WriteIndented = true
	        });

	        this.Publish(body,eventName);
        }

        public void Publish(byte[] content, string eventName)
        {
	        if (!_persistentConnection.IsConnected)
	        {
		        _persistentConnection.TryConnect();
	        }
	        
	        using (var channel = _persistentConnection.CreateModel())
	        {
		        channel.ExchangeDeclare(exchange: _exchange, type: "direct");
		        
		        var properties = channel.CreateBasicProperties();
		        properties.DeliveryMode = 2; // persistent

		        channel.BasicPublish(
			        exchange: _exchange,
			        routingKey: eventName,
			        mandatory: true,
			        basicProperties: properties,
			        body: content);
	        }
        }
        
        public void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = _subsManager.GetEventKey<T>();
            DoInternalSubscription(eventName);

            _subsManager.AddSubscription<T, TH>();
            StartBasicConsume();
        }

        private void DoInternalSubscription(string eventName)
		{
			var containsKey = _subsManager.HasSubscriptionsForEvent(eventName);
			if (containsKey)
			{
				return;
			}

			if (!_persistentConnection.IsConnected)
			{
				_persistentConnection.TryConnect();
			}

			_consumerChannel.QueueBind(queue: _queueName,
								exchange: _exchange,
								routingKey: eventName);
		}

		public void Unsubscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            _subsManager.RemoveSubscription<T, TH>();
        }

        public void Dispose()
        {
            if (_consumerChannel != null)
            {
                _consumerChannel.Dispose();
            }

            _subsManager.Clear();
        }

        private void StartBasicConsume()
		{
			if (_consumerChannel == null)
			{
				return;
			}

			var consumer = new AsyncEventingBasicConsumer(_consumerChannel);

			consumer.Received += Consumer_Received;

			_consumerChannel.BasicConsume(
				queue: _queueName,
				autoAck: false,
				consumer: consumer);
		}

        private void SubsManager_OnEventRemoved(object sender, string eventName)
        {
	        if (!_persistentConnection.IsConnected)
	        {
		        _persistentConnection.TryConnect();
	        }

	        using (var channel = _persistentConnection.CreateModel())
	        {
		        channel.QueueUnbind(queue: _queueName,
			        exchange: _exchange,
			        routingKey: eventName);

		        if (_subsManager.IsEmpty)
		        {
			        _queueName = string.Empty;
			        _consumerChannel.Close();
		        }
	        }
        }
        
		private async Task Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            var eventName = eventArgs.RoutingKey;
            var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

            await ProcessEvent(eventName, message);

            //TODO implement Dead Letter Exchange (DLX). https://www.rabbitmq.com/dlx.html
            _consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);
        }

        private IModel CreateConsumerChannel()
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            var channel = _persistentConnection.CreateModel();

            channel.ExchangeDeclare(exchange: _exchange,
                                    type: "direct");

            channel.QueueDeclare(queue: _queueName,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            channel.CallbackException += (sender, ea) =>
            {
                _consumerChannel.Dispose();
                _consumerChannel = CreateConsumerChannel();
                StartBasicConsume();
            };

            return channel;
        }

        private async Task ProcessEvent(string eventName, string message)
		{
			if (!_subsManager.HasSubscriptionsForEvent(eventName))
			{
				return;
			}

			var subscriptionsHandlersTypes = _subsManager.GetHandlersForEvent(eventName);
			foreach (var handlerType in subscriptionsHandlersTypes)
			{
				var handler = _serviceProvider.GetService(serviceType: handlerType);

				if (handler == null)
				{
					continue;
				}

				var eventType = _subsManager.GetEventTypeByName(eventName);
				var integrationEvent = JsonSerializer.Deserialize(
					message,
					eventType,
					new JsonSerializerOptions()
					{
						PropertyNameCaseInsensitive = true
					}
				);

				var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);

				await Task.Yield();

				await (Task)concreteType
					.GetMethod("Handle")
					.Invoke(handler, new object[] { integrationEvent });
			}
		}
	}
}
