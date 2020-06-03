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

using System.Linq;
using Cassandra.DataStax.Insights.InfoProviders;
using Cassandra.DataStax.Insights.Schema;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.SessionManagement;

namespace Cassandra.DataStax.Insights.MessageFactories
{
    internal class InsightsStartupMessageFactory : IInsightsMessageFactory<InsightsStartupData>
    {
        private const string StartupMessageName = "driver.startup";
        private const string StartupV1MappingId = "v1";
        
        private readonly IInsightsMetadataFactory _metadataFactory;
        private readonly InsightsInfoProvidersCollection _infoProviders;

        public InsightsStartupMessageFactory(IInsightsMetadataFactory metadataFactory, InsightsInfoProvidersCollection infoProviders)
        {
            _metadataFactory = metadataFactory;
            _infoProviders = infoProviders;
        }

        public Insight<InsightsStartupData> CreateMessage(IInternalSession session)
        {
            var metadata = _metadataFactory.CreateInsightsMetadata(
                InsightsStartupMessageFactory.StartupMessageName, InsightsStartupMessageFactory.StartupV1MappingId, InsightType.Event);

            var driverInfo = _infoProviders.DriverInfoProvider.GetInformation(session);
            var startupData = new InsightsStartupData
            {
                ClientId = session.Configuration.ClusterId.ToString(),
                SessionId = session.InternalSessionId.ToString(),
                DriverName = driverInfo.DriverName,
                DriverVersion = driverInfo.DriverVersion,
                ApplicationName = session.Configuration.ApplicationName,
                ApplicationVersion = session.Configuration.ApplicationVersion,
                ApplicationNameWasGenerated = session.Configuration.ApplicationNameWasGenerated,
                ContactPoints = 
                    session.Metadata.ResolvedContactPoints.ToDictionary(
                        kvp => kvp.Key.StringRepresentation, kvp => kvp.Value.Select(ipEndPoint => ipEndPoint.GetHostIpEndPointWithFallback().ToString()).ToList()),
                DataCenters = _infoProviders.DataCentersInfoProvider.GetInformation(session),
                InitialControlConnection = session.Metadata.ControlConnection.EndPoint?.GetHostIpEndPointWithFallback().ToString(),
                LocalAddress = session.Metadata.ControlConnection.LocalAddress?.ToString(),
                HostName = _infoProviders.HostnameProvider.GetInformation(session),
                ProtocolVersion = (byte)session.Metadata.ControlConnection.ProtocolVersion,
                ExecutionProfiles = _infoProviders.ExecutionProfileInfoProvider.GetInformation(session),
                PoolSizeByHostDistance = _infoProviders.PoolSizeByHostDistanceInfoProvider.GetInformation(session),
                HeartbeatInterval = 
                    session
                        .Configuration
                        .GetOrCreatePoolingOptions(session.Metadata.ControlConnection.ProtocolVersion)
                        .GetHeartBeatInterval() ?? 0,
                Compression = session.Configuration.ProtocolOptions.Compression,
                ReconnectionPolicy = _infoProviders.ReconnectionPolicyInfoProvider.GetInformation(session),
                Ssl = new SslInfo { Enabled = session.Configuration.ProtocolOptions.SslOptions != null },
                AuthProvider = _infoProviders.AuthProviderInfoProvider.GetInformation(session),
                OtherOptions = _infoProviders.OtherOptionsInfoProvider.GetInformation(session),
                PlatformInfo = _infoProviders.PlatformInfoProvider.GetInformation(session),
                ConfigAntiPatterns = _infoProviders.ConfigAntiPatternsInfoProvider.GetInformation(session),
                PeriodicStatusInterval = session.Configuration.MonitorReportingOptions.StatusEventDelayMilliseconds / 1000
            };

            return new Insight<InsightsStartupData>
            {
                Metadata = metadata,
                Data = startupData
            };
        }
    }
}