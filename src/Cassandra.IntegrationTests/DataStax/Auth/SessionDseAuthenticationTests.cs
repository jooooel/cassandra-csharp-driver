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
using System.Linq;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.DataStax.Auth
{
    /// <summary>
    /// Test Cassandra Authentication.
    /// </summary>
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.ServerApi)]
    public class SessionDseAuthenticationTests : TestGlobals
    {
        private Lazy<ITestCluster> _testClusterForDseAuthTesting;
        private ISession _session;

        public void RetryUntilClusterAuthHealthy(ITestCluster cluster)
        {
            TestHelper.RetryAssert(
                () =>
                {
                    var ex = Assert.Throws<NoHostAvailableException>(
                        () => SessionBuilder()
                              .AddContactPoint(cluster.InitialContactPoint)
                              .WithAuthProvider(new PlainTextAuthProvider("wrong_username", "password"))
                              .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(22000).SetConnectTimeoutMillis(60000))
                              .Build());
                    Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
                },
                500,
                300);
        }

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            _testClusterForDseAuthTesting = new Lazy<ITestCluster>(() =>
            {
                var cluster = GetTestCcmClusterForAuthTests();
                //Wait 10 seconds as auth table needs to be created
                Thread.Sleep(10000);
                RetryUntilClusterAuthHealthy(cluster);
                return cluster;
            });
        }

        [TearDown]
        public void TearDown()
        {
            _session?.Dispose();
            _session = null;
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown()
        {
            if (_testClusterForDseAuthTesting.IsValueCreated)
            {
                TestClusterManager.TryRemove();
            }
        }

        private ITestCluster GetTestCcmClusterForAuthTests()
        {
            return
                TestClusterManager.CreateNew(1, new TestClusterOptions
                {
                    DseYaml = new[] { "authentication_options.default_scheme: internal", "authentication_options.enabled: true" },
                    CassandraYaml = new[] { "authenticator: com.datastax.bdp.cassandra.auth.DseAuthenticator" },
                    JvmArgs = new[] { "-Dcassandra.superuser_setup_delay_ms=0" }
                });
        }

        [Test, TestDseVersion(5, 0)]
        public void StandardCreds_DseAuth_AuthSuccess()
        {
            var builder = SessionBuilder()
                .AddContactPoint(_testClusterForDseAuthTesting.Value.InitialContactPoint)
                .WithCredentials("cassandra", "cassandra");
            _session = builder.Build();

            var rs = _session.Execute("SELECT * FROM system.local");
            Assert.Greater(rs.Count(), 0);
        }

        [Test, TestDseVersion(5, 0)]
        public void StandardCreds_DseAuth_AuthFail()
        {
            var ex = Assert.Throws<NoHostAvailableException>(
                () => SessionBuilder()
                      .AddContactPoint(_testClusterForDseAuthTesting.Value.InitialContactPoint)
                      .WithCredentials("wrong_username", "password")
                      .Build());
            Assert.AreEqual(1, ex.Errors.Count);
            Assert.IsTrue(ex.Message.Contains("Failed to login. Please re-try."), ex.Message);
            Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
        }

        [Test, TestDseVersion(5, 0)]
        public void StandardCreds_DseAuth_AuthOmitted()
        {
            var ex = Assert.Throws<NoHostAvailableException>(
                () => SessionBuilder()
                      .AddContactPoint(_testClusterForDseAuthTesting.Value.InitialContactPoint)
                      .Build());
            Assert.AreEqual(1, ex.Errors.Count);
            Assert.IsTrue(ex.Message.Contains("requires authentication, but no authenticator found in Cluster configuration"));
            Assert.IsInstanceOf<AuthenticationException>(ex.Errors.First().Value);
        }
    }
}