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
using System.Linq;
using System.Net;
using Cassandra.Connections;
using Cassandra.DataStax.Graph;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class BuilderTests : BaseUnitTest
    {
        [Test]
        public void WithConnectionStringCredentialsTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Session.Builder().WithConnectionString(string.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Session.Builder().WithConnectionString(string.Format("Contact Points={0};Username=user1", contactPoints));
            config = builder.GetConfiguration();
            //As there is no password, auth provider should be empty
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Session.Builder().WithConnectionString(string.Format("Contact Points={0};Username=user1;Password=P@ssword!", contactPoints));
            config = builder.GetConfiguration();
            Assert.IsInstanceOf<PlainTextAuthProvider>(config.AuthProvider);
            Assert.IsInstanceOf<SimpleAuthInfoProvider>(config.AuthInfoProvider);
        }

        [Test]
        public void WithConnectionStringPortTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Session.Builder().WithConnectionString(string.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.AreEqual(config.ProtocolOptions.Port, ProtocolOptions.DefaultPort);

            builder = Session.Builder().WithConnectionString(string.Format("Contact Points={0};Port=9000", contactPoints));
            config = builder.GetConfiguration();
            Assert.AreEqual(config.ProtocolOptions.Port, 9000);
        }

        [Test]
        public void WithConnectionStringDefaultKeyspaceTest()
        {
            const string contactPoints = "127.0.0.1,127.0.0.2,127.0.0.3";
            var builder = Session.Builder().WithConnectionString(string.Format("Contact Points={0}", contactPoints));
            var config = builder.GetConfiguration();
            Assert.IsNull(config.ClientOptions.DefaultKeyspace);

            builder = Session.Builder().WithConnectionString(string.Format("Contact Points={0};Default Keyspace=ks1", contactPoints));
            config = builder.GetConfiguration();
            Assert.AreEqual(config.ClientOptions.DefaultKeyspace, "ks1");
        }

        [Test]
        public void WithCredentials()
        {
            var contactPoints = new string[] { "127.0.0.1", "127.0.0.2", "127.0.0.3" };
            var builder = Session.Builder().AddContactPoints(contactPoints);
            var config = builder.GetConfiguration();
            Assert.IsInstanceOf<NoneAuthProvider>(config.AuthProvider);
            Assert.IsNull(config.AuthInfoProvider);

            builder = Session.Builder().AddContactPoints(contactPoints).WithCredentials("user1", "password");
            config = builder.GetConfiguration();
            Assert.IsInstanceOf<PlainTextAuthProvider>(config.AuthProvider);
            Assert.IsInstanceOf<SimpleAuthInfoProvider>(config.AuthInfoProvider);

            Exception ex = Assert.Throws<ArgumentNullException>(() =>
                Session.Builder().AddContactPoints(contactPoints).WithCredentials("user1", null));
            Assert.That(ex.Message, Contains.Substring("password"));

            ex = Assert.Throws<ArgumentNullException>(() =>
                Session.Builder().AddContactPoints(contactPoints).WithCredentials(null, null));
            Assert.That(ex.Message, Contains.Substring("username"));
        }
        
        [Test]
        public void Should_SetResolvedContactPoints_When_SessionIsBuilt()
        {
            const string host1 = "127.0.0.1";
            const string host2 = "127.0.0.2";
            const string host3 = "localhost";
            
            var builder = Session.Builder().AddContactPoints(host1, host2, host3);
            var session = builder.BuildInternal();
            Assert.AreEqual(3, session.GetResolvedEndpoints().Count);
            CollectionAssert.AreEqual(
                new[] { new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse(host1), ProtocolOptions.DefaultPort), session.Configuration.ServerNameResolver, null) }, 
                session.GetResolvedEndpoints().Single(kvp => kvp.Key.StringRepresentation == host1).Value);
            CollectionAssert.AreEqual(
                new[] { new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse(host2), ProtocolOptions.DefaultPort), session.Configuration.ServerNameResolver, null) }, 
                session.GetResolvedEndpoints().Single(kvp => kvp.Key.StringRepresentation == host2).Value);

            var localhostAddress = new ConnectionEndPoint(new IPEndPoint(IPAddress.Parse("127.0.0.1"), ProtocolOptions.DefaultPort), session.Configuration.ServerNameResolver, null);
            Assert.Contains(localhostAddress, session.GetResolvedEndpoints()
                                                     .Single(kvp => kvp.Key.StringRepresentation == host3)
                                                     .Value
                                                     .ToList());
        }

        [Test]
        public void WithMaxProtocolVersion_Sets_Configuration_MaxProtocolVersion()
        {
            var builder = Session.Builder()
                .AddContactPoint("192.168.1.10")
                .WithMaxProtocolVersion(100);
            var session = builder.Build();
            Assert.AreEqual(100, session.Configuration.ProtocolOptions.MaxProtocolVersion);
            builder = Session.Builder()
                .AddContactPoint("192.168.1.10")
                .WithMaxProtocolVersion(3);
            session = builder.Build();
            Assert.AreEqual(ProtocolVersion.V3, session.Configuration.ProtocolOptions.MaxProtocolVersionValue);
            builder = Session.Builder()
                .AddContactPoint("192.168.1.10")
                .WithMaxProtocolVersion(ProtocolVersion.V2);
            session = builder.Build();
            Assert.AreEqual(ProtocolVersion.V2, session.Configuration.ProtocolOptions.MaxProtocolVersionValue);
        }

        [Test]
        public void MaxProtocolVersion_Defaults_To_Session_Max()
        {
            var builder = Session.Builder()
                .AddContactPoint("192.168.1.10");
            var session = builder.Build();
            Assert.AreEqual(Session.MaxProtocolVersion, session.Configuration.ProtocolOptions.MaxProtocolVersion);
            //Defaults to null
            Assert.Null(new ProtocolOptions().MaxProtocolVersion);
        }

        [Test]
        public void WithMaxProtocolVersion_Validates_Greater_Than_Zero()
        {
            Assert.Throws<ArgumentException>(() => Session.Builder()
                .AddContactPoint("192.168.1.10")
                .WithMaxProtocolVersion((byte)0));
        }

        [Test]
        [TestCase(ProtocolVersion.MaxSupported, 1, 2)]
        [TestCase(ProtocolVersion.V2, 2, 8)]
        public void PoolingOptions_Create_Based_On_Protocol_Version(ProtocolVersion protocolVersion,
            int coreConnections, int maxConnections)
        {
            var options1 = PoolingOptions.Create(protocolVersion);
            var session1 = Session.Builder()
                                  .AddContactPoint("::1")
                                  .WithPoolingOptions(options1)
                                  .Build();
            Assert.AreEqual(coreConnections, session1.Configuration.PoolingOptions.GetCoreConnectionsPerHost(HostDistance.Local));
            Assert.AreEqual(maxConnections, session1.Configuration.PoolingOptions.GetMaxConnectionPerHost(HostDistance.Local));
        }

        [Test]
        public void Session_Builder_Should_Use_Default_ContactPoint_When_No_Contact_Points_Have_Been_Defined()
        {
            var ex = Assert.Throws<NoHostAvailableException>(() => Session.Builder().Build());
            Assert.AreEqual(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042), ex.Errors.Single().Key);
        }

        [Test]
        public void Builder_Build_Throws_When_Name_Could_Not_Be_Resolved()
        {
            const string hostName = "not-a-host";
            var builder = Session.Builder().AddContactPoint(hostName);
            var ex = Assert.Throws<NoHostAvailableException>(() => builder.Build());
            Assert.That(ex.Message, Does.StartWith("No host name could be resolved"));
        }

        [Test]
        public void Should_Throw_When_All_Contact_Points_Cant_Be_Resolved()
        {
            var ex = Assert.Throws<NoHostAvailableException>(() => Session.Builder()
                .AddContactPoint("not-a-host")
                .AddContactPoint("not-a-host2")
                .Build());
            Assert.That(ex.Message, Is.EqualTo("No host name could be resolved, attempted: not-a-host, not-a-host2"));
        }

        [Test]
        public void Session_Builder_Should_Use_Provided_Port()
        {
            const int port = 9099;
            var endpoint = new IPEndPoint(IPAddress.Parse("127.0.0.2"), port);

            // Provided as string
            var ex = Assert.Throws<NoHostAvailableException>(
                () => Session.Builder()
                             .AddContactPoint(endpoint.Address.ToString())
                             .WithPort(port)
                             .Build());
            Assert.That(ex.Errors.Count, Is.EqualTo(1));
            Assert.That(ex.Errors.Keys.First(), Is.EqualTo(endpoint));

            // Provided as an IPAddress
            ex = Assert.Throws<NoHostAvailableException>(
                () => Session.Builder()
                             .AddContactPoint(endpoint.Address)
                             .WithPort(port)
                             .Build());
            Assert.That(ex.Errors.Count, Is.EqualTo(1));
            Assert.That(ex.Errors.Keys.First(), Is.EqualTo(endpoint));
        }

        [Test]
        public void Session_Builder_Returns_Contact_Points_Provided_As_IPEndPoint_Instances()
        {
            var endpoint1 = new IPEndPoint(0x7000001L, 9042);
            var endpoint2 = new IPEndPoint(0x7000002L, 9042);
            var address = IPAddress.Parse("10.10.10.1");
            var addressString = "10.10.10.2";
            var builder = Session.Builder().AddContactPoint(endpoint1).AddContactPoint(address)
                                 .AddContactPoint(addressString).AddContactPoint(endpoint2);

            // Only IPEndPoint instances as IP addresses and host names must be resolved and assigned
            // the port number, which is performed on Build()
            Assert.AreEqual(new[] { endpoint1, endpoint2 }, builder.ContactPoints);
        }

        [Test]
        [TestCase(0)]
        [TestCase(-1)]
        public void Should_ThrowArgumentException_When_ProvidedMaxSchemaAgreementsWaitSecondsIsInvalid(int seconds)
        {
            var builder = Session.Builder();
            var ex = Assert.Throws<ArgumentException>(() => builder.WithMaxSchemaAgreementWaitSeconds(seconds));
            Assert.That(ex.Message, Is.EqualTo("Max schema agreement wait must be greater than zero"));
        }

        [Test]
        public void Should_ReturnCorrectMaxSchemaAgreementsWaitSeconds_When_ValueIsProvidedToBuilder()
        {
            var expected = 20;
            var config = Session.Builder()
                                 .AddContactPoint("192.168.1.10")
                                 .WithMaxSchemaAgreementWaitSeconds(expected)
                                 .GetConfiguration();
            Assert.AreEqual(expected, config.ProtocolOptions.MaxSchemaAgreementWaitSeconds);
        }

        [Test]
        public void Should_ReturnDefaultMaxSchemaAgreementWaitSeconds_When_NotProvidedToBuilder()
        {
            var expected = ProtocolOptions.DefaultMaxSchemaAgreementWaitSeconds;
            var config = Session.Builder()
                                 .AddContactPoint("192.168.1.10")
                                 .GetConfiguration();
            Assert.AreEqual(expected, config.ProtocolOptions.MaxSchemaAgreementWaitSeconds);
        }

        [Test]
        public void Should_ThrowException_When_ContactPointAndBundleAreProvided()
        {
            const string exceptionMsg = "Contact points can not be set when a secure connection bundle is provided.";
            var builder = Session.Builder()
                                .AddContactPoint("192.168.1.10")
                                .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Session.Builder()
                                 .AddContactPoint(IPAddress.Parse("192.168.1.10"))
                                 .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Session.Builder()
                             .AddContactPoint(new IPEndPoint(IPAddress.Parse("192.168.1.10"), 9042))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Session.Builder()
                             .AddContactPoints(new IPEndPoint(IPAddress.Parse("192.168.1.10"), 9042))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = Session.Builder()
                             .AddContactPoint(IPAddress.Parse("192.168.1.10"))
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = Session.Builder()
                             .WithCloudSecureConnectionBundle("bundle")
                             .AddContactPoint(IPAddress.Parse("192.168.1.10"));
            
            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_SslOptionsAndBundleAreProvided()
        {
            const string exceptionMsg = "SSL options can not be set when a secure connection bundle is provided.";
            var builder = Session.Builder()
                                .WithSSL()
                                .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Session.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Session.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Session.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = Session.Builder()
                             .WithSSL()
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);

            builder = Session.Builder()
                             .WithCloudSecureConnectionBundle("bundle")
                             .WithSSL();
            
            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }
        
        [Test]
        public void Should_ThrowException_When_SslOptionsAndContactPointAndBundleAreProvided()
        {
            const string exceptionMsg = "SSL options can not be set when a secure connection bundle is provided.";
            var builder = Session.Builder()
                                 .AddContactPoints("127.0.0.1")
                                 .WithSSL()
                                 .WithCloudSecureConnectionBundle("bundle");

            var ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
            
            builder = Session.Builder()
                             .WithSSL()
                             .AddContactPoints("127.0.0.1")
                             .WithCloudSecureConnectionBundle("bundle");

            ex = Assert.Throws<ArgumentException>(() => builder.Build());
            Assert.AreEqual(exceptionMsg, ex.Message);
        }

        [Test]
        public void Should_Build_A_Session_With_Graph_Options()
        {
            var graphOptions = new GraphOptions();
            var session = Session.Builder()
                .WithGraphOptions(graphOptions)
                .AddContactPoint("192.168.1.159")
                .Build();
            Assert.NotNull(session.Configuration);
            Assert.AreSame(graphOptions, session.Configuration.GraphOptions);
        }

        [Test]
        public void Should_Build_A_Session_With_Default_Graph_Options()
        {
            //without specifying graph options
            var session = Session.Builder().AddContactPoint("192.168.1.159").Build();
            Assert.NotNull(session.Configuration);
            Assert.NotNull(session.Configuration);
            Assert.NotNull(session.Configuration.GraphOptions);
        }

        [Test]
        public void Should_Build_A_Session_With_DefaultLoadBalancingPolicy()
        {
            //without specifying load balancing policy
            var session = Session.Builder().AddContactPoint("192.168.1.159").Build();
            Assert.NotNull(session.Configuration);
            Assert.IsInstanceOf<DefaultLoadBalancingPolicy>(
                session.Configuration.Policies.LoadBalancingPolicy);
        }

        [Test]
        public void Should_Build_A_Session_With_The_Specified_LoadBalancingPolicy()
        {
            var lbp = new TestLoadBalancingPolicy();
            var session = Session.Builder()
                .AddContactPoint("192.168.1.159")
                .WithLoadBalancingPolicy(lbp)
                .Build();
            Assert.NotNull(session.Configuration);
            Assert.AreSame(lbp, session.Configuration.Policies.LoadBalancingPolicy);
        }

        [Test]
        public void Should_ReturnDefaultInsightsMonitoringEnabled_When_NotProvidedToBuilder()
        {
            const bool expected = MonitorReportingOptions.DefaultMonitorReportingEnabled;
            var session = Session.Builder()
                                .AddContactPoint("192.168.1.10")
                                .Build();
            Assert.AreEqual(expected, session.Configuration.MonitorReportingOptions.MonitorReportingEnabled);
            Assert.AreEqual(MonitorReportingOptions.DefaultStatusEventDelayMilliseconds, session.Configuration.MonitorReportingOptions.StatusEventDelayMilliseconds);
        }
    }
}