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
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Tasks;

namespace Cassandra.Data
{
    /// <summary>
    /// Represents a CQL connection.
    /// </summary>
    /// <inheritdoc cref="DbConnection" />
    public class CqlConnection : DbConnection, ICloneable
    {
        private CassandraConnectionStringBuilder _connectionStringBuilder;

        private static readonly ConcurrentDictionary<string, ISession> Sessions =
            new ConcurrentDictionary<string, ISession>();

        private ConnectionState _connectionState = ConnectionState.Closed;
        private CqlBatchTransaction _currentTransaction;

        /// <summary>
        /// Initializes a <see cref="CqlConnection"/>.
        /// </summary>
        public CqlConnection()
        {
            _connectionStringBuilder = new CassandraConnectionStringBuilder();
        }

        /// <summary>
        /// Initializes a <see cref="CqlConnection"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public CqlConnection(string connectionString)
        {
            _connectionStringBuilder = new CassandraConnectionStringBuilder(connectionString);
        }

        protected internal ISession ManagedConnection { get; internal set; }

        internal void ClearDbTransaction()
        {
            _currentTransaction = null;
        }

        /// <inheritdoc />
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (_currentTransaction != null)
            {
                throw new InvalidOperationException();
            }

            _currentTransaction = new CqlBatchTransaction(this);
            return _currentTransaction;
        }

        /// <inheritdoc />
        public override void ChangeDatabase(string databaseName)
        {
            ManagedConnection?.ChangeKeyspace(databaseName);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (_connectionState == ConnectionState.Open)
            {
                Close();
            }

            base.Dispose(disposing);
        }

        /// <inheritdoc />
        public override void Close()
        {
            _connectionState = ConnectionState.Closed;
            if (ManagedConnection != null)
            {
                ManagedConnection.Dispose();
                ManagedConnection = null;
            }
        }

        /// <inheritdoc />
        public override string ConnectionString
        {
            get => _connectionStringBuilder?.ConnectionString;
            set => _connectionStringBuilder = new CassandraConnectionStringBuilder(value);
        }

        /// <inheritdoc />
        protected override DbCommand CreateDbCommand()
        {
            var cmd = new CqlCommand { CqlConnection = this };
            _currentTransaction?.Append(cmd);
            return cmd;
        }

        /// <inheritdoc />
        public override string DataSource => _connectionStringBuilder.ClusterName;

        /// <summary>
        /// Returns the Keyspace
        /// </summary>
        public override string Database => ManagedConnection?.Keyspace;

        protected override DbProviderFactory DbProviderFactory => CqlProviderFactory.Instance;

        /// <inheritdoc />
        public override void Open()
        {
            TaskHelper.WaitToComplete(OpenAsync(default));
        }

        /// <inheritdoc />
        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            _connectionState = ConnectionState.Connecting;
            ManagedConnection = await CreateSessionAsync(_connectionStringBuilder).ConfigureAwait(false);
            _connectionState = ConnectionState.Open;
        }

        /// <summary>
        /// To be overridden in child classes to change the default <see cref="Builder"/> settings
        /// for building a <see cref="ISession"/>.
        ///
        /// For example, some clients might want to specify the <see cref="DCAwareRoundRobinPolicy"/>
        /// when building the <see cref="ISession"/> so that the clients could talk to only the hosts
        /// in specified datacenter for better performance.
        /// </summary>
        /// <param name="builder">The <see cref="Builder"/> for building a <see cref="ISession"/>.</param>
        protected virtual void OnBuildingSession(Builder builder)
        {
        }

        /// <summary>
        /// Creates a <see cref="ISession"/>. By default <see cref="ISession"/>s are created and cached
        /// by cluster name specified in connection string.
        ///
        /// To be overridden in child classes to change the default creation and caching behavior.
        /// </summary>
        /// <param name="connectionStringBuilder">The <see cref="CassandraConnectionStringBuilder"/>.</param>
        /// <returns></returns>
        protected virtual async Task<ISession> CreateSessionAsync(CassandraConnectionStringBuilder connectionStringBuilder)
        {
            if (!CqlConnection.Sessions.TryGetValue(_connectionStringBuilder.ClusterName, out var session))
            {
                var builder = _connectionStringBuilder.MakeSessionBuilder();
                OnBuildingSession(builder);
                session = await builder.BuildAsync().ConfigureAwait(false);
                CqlConnection.Sessions.TryAdd(_connectionStringBuilder.ClusterName, session);
            }

            return session;
        }

        /// <summary>
        /// To be called by CqlCommand to creates a <see cref="PreparedStatement"/>
        /// from <see cref="ManagedConnection"/>.
        ///
        /// To be overridden in child classes if want to cache the <see cref="PreparedStatement"/> created.
        /// </summary>
        /// <param name="cqlQuery">The CQL query string.</param>
        /// <returns>Returns the created <see cref="PreparedStatement"/>.</returns>
        protected internal virtual PreparedStatement CreatePreparedStatement(string cqlQuery)
        {
            return ManagedConnection?.Prepare(cqlQuery);
        }

        /// <inheritdoc />
        public override string ServerVersion => "2.0";

        /// <inheritdoc />
        public override ConnectionState State => _connectionState;

        public object Clone()
        {
            var conn = new CqlConnection(_connectionStringBuilder.ConnectionString);
            if (State != ConnectionState.Closed && State != ConnectionState.Broken)
            {
                conn.Open();
            }

            return conn;
        }
    }
}