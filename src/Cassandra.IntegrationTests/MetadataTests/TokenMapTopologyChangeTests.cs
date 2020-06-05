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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.MetadataTests
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealClusterLong)]
    public class TokenMapTopologyChangeTests : TestGlobals
    {
        private ITestCluster TestCluster { get; set; }

        private ISession SessionObjSync { get; set; }

        private ISession SessionObjNotSync { get; set; }

        [Test]
        public void TokenMap_Should_RebuildTokenMap_When_NodeIsDecommissioned()
        {
            var listener = new TestTraceListener();
            var level = Diagnostics.CassandraTraceSwitch.Level;
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            Trace.Listeners.Add(listener);
            try
            {
                TestCluster = TestClusterManager.CreateNew(3, new TestClusterOptions { UseVNodes = true });
                var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
                SessionObjSync = SessionBuilder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(true))
                                        .WithReconnectionPolicy(new ConstantReconnectionPolicy(5000))
                                        .Build();

                SessionObjNotSync = SessionBuilder()
                                           .AddContactPoint(TestCluster.InitialContactPoint)
                                           .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                                           .WithReconnectionPolicy(new ConstantReconnectionPolicy(5000))
                                           .Build();
                
                var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
                SessionObjNotSync.Execute(createKeyspaceCql);

                TestUtils.WaitForSchemaAgreement(SessionObjNotSync);
                TestUtils.WaitForSchemaAgreement(SessionObjSync);

                SessionObjNotSync.ChangeKeyspace(keyspaceName);
                SessionObjSync.ChangeKeyspace(keyspaceName);

                ICollection<Host> replicasSync = null;
                ICollection<Host> replicasNotSync = null;

                TestHelper.RetryAssert(() =>
                {
                    Assert.AreEqual(3, SessionObjSync.Metadata.Hosts.Count);
                    Assert.AreEqual(3, SessionObjNotSync.Metadata.Hosts.Count);

                    replicasSync = SessionObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                    replicasNotSync = SessionObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));

                    Assert.AreEqual(3, replicasSync.Count);
                    Assert.AreEqual(1, replicasNotSync.Count);
                }, 100, 150);

                var oldTokenMapNotSync = SessionObjNotSync.Metadata.TokenToReplicasMap;
                var oldTokenMapSync = SessionObjSync.Metadata.TokenToReplicasMap;
                
                if (TestClusterManager.SupportsDecommissionForcefully())
                {
                    this.TestCluster.DecommissionNodeForcefully(1);
                }
                else
                {
                    this.TestCluster.DecommissionNode(1);
                }

                this.TestCluster.Stop(1);

                TestHelper.RetryAssert(() =>
                {
                    Assert.AreEqual(2, SessionObjSync.Metadata.Hosts.Count, "ClusterObjSync.Metadata.Hosts.Count");
                    Assert.AreEqual(2, SessionObjNotSync.Metadata.Hosts.Count, "ClusterObjNotSync.Metadata.Hosts.Count");

                    replicasSync = SessionObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                    replicasNotSync = SessionObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));

                    Assert.AreEqual(2, replicasSync.Count, "replicasSync.Count");
                    Assert.AreEqual(1, replicasNotSync.Count, "replicasNotSync.Count");

                    Assert.IsFalse(object.ReferenceEquals(SessionObjNotSync.Metadata.TokenToReplicasMap, oldTokenMapNotSync));
                    Assert.IsFalse(object.ReferenceEquals(SessionObjSync.Metadata.TokenToReplicasMap, oldTokenMapSync));
                }, 1000, 360);

                oldTokenMapNotSync = SessionObjNotSync.Metadata.TokenToReplicasMap;
                oldTokenMapSync = SessionObjSync.Metadata.TokenToReplicasMap;

                this.TestCluster.BootstrapNode(4);
                TestHelper.RetryAssert(() =>
                {
                    Assert.AreEqual(3, SessionObjSync.Metadata.Hosts.Count);
                    Assert.AreEqual(3, SessionObjNotSync.Metadata.Hosts.Count);

                    replicasSync = SessionObjSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));
                    replicasNotSync = SessionObjNotSync.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123"));

                    Assert.AreEqual(3, replicasSync.Count);
                    Assert.AreEqual(1, replicasNotSync.Count);

                    Assert.IsFalse(object.ReferenceEquals(SessionObjNotSync.Metadata.TokenToReplicasMap, oldTokenMapNotSync));
                    Assert.IsFalse(object.ReferenceEquals(SessionObjSync.Metadata.TokenToReplicasMap, oldTokenMapSync));
                }, 1000, 360);

            }
            catch (Exception ex)
            {
                Trace.Flush();
                Assert.Fail("Exception: " + ex.ToString() + Environment.NewLine + string.Join(Environment.NewLine, listener.Queue.ToArray()));
            }
            finally
            {
                Trace.Listeners.Remove(listener);
                Diagnostics.CassandraTraceSwitch.Level = level;
            }
        }

        [TearDown]
        public void TearDown()
        {
            SessionObjSync?.Shutdown();
            SessionObjNotSync?.Shutdown();
            TestCluster?.Remove();
        }
    }
}