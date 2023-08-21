//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Diagnostics;
using System.Threading;
using Cassandra.Metrics.Abstractions;
using Cassandra.Metrics.Internal;
using Cassandra.Metrics.Registries;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;

namespace Cassandra.Observers
{
    internal class TelemetryRequestObserver : IRequestObserver
    {
        private readonly IRequest _request;
#if NETSTANDARD
        private static readonly ActivitySource MyActivitySource = new ActivitySource("Cassandra", "1.0.0");
        private readonly Activity _activity;
#endif

        public TelemetryRequestObserver(IRequest request)
        {
            // TODO: HANTERA ANDRA TYPER AV REQUESTS?
            _request = request;
#if NETSTANDARD
            _activity = MyActivitySource.CreateActivity("SomeActivity", ActivityKind.Client);

            if (request is QueryRequest queryRequest)
            {
                _activity?.SetTag("cql", queryRequest.Query);
            }
#endif
        }

        public void OnSpeculativeExecution(Host host, long delay)
        {
        }

        public void OnRequestError(Host host, RequestErrorType errorType, RetryDecision.RetryDecisionType decision)
        {
#if NETSTANDARD
            // _activity
#endif
            // var nodeMetrics = _manager.GetOrCreateNodeMetrics(host);
            // OnRequestError(nodeMetrics.Errors, errorType);
            // switch (decision)
            // {
            //     case RetryDecision.RetryDecisionType.Retry:
            //         OnRetryPolicyDecision(nodeMetrics.Retries, errorType);
            //         break;
            //
            //     case RetryDecision.RetryDecisionType.Ignore:
            //         OnRetryPolicyDecision(nodeMetrics.Ignores, errorType);
            //         break;
            // }
        }

        private void OnRetryPolicyDecision(IRetryPolicyMetrics metricsRegistry, RequestErrorType reason)
        {
            // metricsRegistry.Total.Increment();
            // switch (reason)
            // {
            //     case RequestErrorType.Unavailable:
            //         metricsRegistry.Unavailable.Increment();
            //         break;
            //
            //     case RequestErrorType.ReadTimeOut:
            //         metricsRegistry.ReadTimeout.Increment();
            //         break;
            //
            //     case RequestErrorType.WriteTimeOut:
            //         metricsRegistry.WriteTimeout.Increment();
            //         break;
            //
            //     case RequestErrorType.Other:
            //     case RequestErrorType.Aborted:
            //     case RequestErrorType.Unsent:
            //     case RequestErrorType.ClientTimeout:
            //         metricsRegistry.Other.Increment();
            //         break;
            //
            //     default:
            //         throw new ArgumentOutOfRangeException(nameof(reason), reason, null);
            // }
        }

        // private void OnRequestError(IRequestErrorMetrics metricsRegistry, RequestErrorType errorType)
        // {
        //     switch (errorType)
        //     {
        //         case RequestErrorType.Unavailable:
        //             metricsRegistry.Unavailable.Increment();
        //             break;
        //
        //         case RequestErrorType.ReadTimeOut:
        //             metricsRegistry.ReadTimeout.Increment();
        //             break;
        //
        //         case RequestErrorType.WriteTimeOut:
        //             metricsRegistry.WriteTimeout.Increment();
        //             break;
        //
        //         case RequestErrorType.Other:
        //             metricsRegistry.Other.Increment();
        //             break;
        //
        //         case RequestErrorType.Aborted:
        //             metricsRegistry.Aborted.Increment();
        //             break;
        //
        //         case RequestErrorType.Unsent:
        //             metricsRegistry.Unsent.Increment();
        //             break;
        //
        //         case RequestErrorType.ClientTimeout:
        //             metricsRegistry.ClientTimeout.Increment();
        //             break;
        //
        //         default:
        //             throw new ArgumentOutOfRangeException(nameof(errorType), errorType, null);
        //     }
        // }

        public void OnRequestStart()
        {
#if NETSTANDARD
            _activity?.Start();
            _activity?.SetTag("TestarIgen", "Hej");
#endif
        }

        public void OnRequestFinish(Exception exception)
        {
#if NETSTANDARD
            _activity.Stop();
            // _activity.SetTag("otel.status_code", "OK");
            // _activity.Dispose();
#endif
        }
    }
}