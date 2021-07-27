using RabbitMQ.Client;
using System;

namespace Soulgram.EventBus.RabbitMq
{
	public class RabbitMqConnection : IRabbitMqConnection
	{
		private readonly IConnectionFactory _connectionFactory;
		private IConnection _connection;
		
		private bool _disposed;
		private readonly object _syncRoot = new object();

		public RabbitMqConnection(IConnectionFactory connectionFactory)
		{
			_connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
		}

		public bool IsConnected => _connection != null && _connection.IsOpen && !_disposed;

		public IModel CreateModel()
		{
			if (!IsConnected)
			{
				throw new InvalidOperationException("No RabbitMQ connections are available to perform this action");
			}

			return _connection.CreateModel();
		}

		public void Dispose()
		{
			if (_disposed)
			{
				return;
			}

			_disposed = true;

			_connection.Dispose();
		}

		public bool TryConnect()
		{
			lock (_syncRoot)
			{
				_connection = _connectionFactory.CreateConnection();

				if (IsConnected)
				{
					_connection.ConnectionShutdown += ReconnectIfNotDisposed;
					_connection.CallbackException += ReconnectIfNotDisposed;
					_connection.ConnectionBlocked += ReconnectIfNotDisposed;

					return true;
				}

				return false;
			}
		}

		private void ReconnectIfNotDisposed(object sender, EventArgs reason)
		{
			if (_disposed)
			{
				return;
			}

			TryConnect();
		}
	}
}
