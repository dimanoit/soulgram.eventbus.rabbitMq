using System;
using RabbitMQ.Client;

namespace Soulgram.EventBus.RabbitMq
{
	public interface IRabbitMQConnection
		: IDisposable
	{
		bool IsConnected { get; }

		bool TryConnect();

		IModel CreateModel();
	}
}