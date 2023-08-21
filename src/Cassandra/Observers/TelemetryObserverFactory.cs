using System;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;

namespace Cassandra.Observers
{
    internal class TelemetryObserverFactory : IObserverFactory
    {
        private readonly IRequest _request;

        public TelemetryObserverFactory(IRequest request)
        {
            _request = request;
        }

        public IRequestObserver CreateRequestObserver()
        {
            return new TelemetryRequestObserver(_request);
        }

        public IConnectionObserver CreateConnectionObserver(Host host)
        {
            throw new NotImplementedException();
        }
    }
}