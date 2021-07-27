using System;
using RabbitMQ.Client;

namespace Soulgram.EventBus.RabbitMq
{
	public interface IRabbitMqConnection
		: IDisposable
	{
		bool IsConnected { get; }

		bool TryConnect();

		IModel CreateModel();
	}
}