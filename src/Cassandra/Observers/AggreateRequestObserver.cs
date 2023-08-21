using System;
using System.Collections.Generic;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;

namespace Cassandra.Observers
{
    internal class AggreateRequestObserver : IRequestObserver
    {
        private readonly IEnumerable<IRequestObserver> _requestObservers;

        public AggreateRequestObserver(IEnumerable<IRequestObserver> requestObservers)
        {
            _requestObservers = requestObservers;
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
            foreach (var requestObserver in _requestObservers)
            {
                requestObserver.OnSpeculativeExecution(host, delay);
            }
        }

        public void OnRequestError(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision)
        {
            foreach (var requestObserver in _requestObservers)
            {
                requestObserver.OnRequestError(host, errorType, decision);
            }
        }

        public void OnRequestStart()
        {
            foreach (var requestObserver in _requestObservers)
            {
                requestObserver.OnRequestStart();
            }
        }

        public void OnRequestFinish(Exception exception)
        {
            foreach (var requestObserver in _requestObservers)
            {
                requestObserver.OnRequestFinish(exception);
            }
        }
    }
}