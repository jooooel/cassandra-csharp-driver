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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.Collections;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Insights;
using Cassandra.ExecutionProfiles;
using Cassandra.Helpers;
using Cassandra.Metrics;
using Cassandra.Metrics.Internal;
using Cassandra.Observers.Abstractions;
using Cassandra.ProtocolEvents;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <inheritdoc cref="ISession" />
    internal class Session : IInternalSession
    {
        internal const ProtocolVersion MaxProtocolVersion = ProtocolVersion.MaxSupported;

        private static readonly IPEndPoint DefaultContactPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
        
        private static long _sessionCounter = -1;

        private volatile bool _initialized;
        private volatile Exception _initException;
        private readonly SemaphoreSlim _initLock = new SemaphoreSlim(1, 1);
        private readonly Metadata _metadata;
        private readonly IControlConnection _controlConnection;
        private readonly IProtocolEventDebouncer _protocolEventDebouncer;
        private readonly bool _implicitContactPoint = false;

        private IReadOnlyList<ILoadBalancingPolicy> _loadBalancingPolicies;

        private readonly ISerializerManager _serializerManager;
        private static readonly Logger Logger = new Logger(typeof(Session));
        private readonly IThreadSafeDictionary<IPEndPoint, IHostConnectionPool> _connectionPool;
        private int _disposed;
        private volatile string _keyspace;
        private readonly IMetricsManager _metricsManager;
        private readonly IObserverFactory _observerFactory;
        private readonly IInsightsClient _insightsClient;

        public int BinaryProtocolVersion => (int)_serializerManager.GetCurrentSerializer().ProtocolVersion;
        
        public IMetricsManager MetricsManager => _metricsManager;

        public IObserverFactory ObserverFactory => _observerFactory;

        /// <inheritdoc />
        public IControlConnection GetControlConnection()
        {
            return _controlConnection;
        }

        /// <summary>
        /// Gets the session configuration
        /// </summary>
        public Configuration Configuration { get; protected set; }

        /// <inheritdoc />
        // ReSharper disable once ConvertToAutoProperty, reviewed
        public bool ImplicitContactPoint => _implicitContactPoint;

        /// <summary>
        /// Determines if the session is already disposed
        /// </summary>
        public bool IsDisposed => Volatile.Read(ref _disposed) > 0;

        /// <summary>
        /// Gets or sets the keyspace
        /// </summary>
        public string Keyspace
        {
            get => _keyspace;
            private set => _keyspace = value;
        }

        /// <summary>
        /// Gets or sets the keyspace
        /// </summary>
        string IInternalSession.Keyspace
        {
            get => Keyspace;
            set => Keyspace = value;
        }

        /// <inheritdoc />
        public UdtMappingDefinitions UserDefinedTypes { get; private set; }

        public string SessionName { get; }

        public Policies Policies => Configuration.Policies;

        /// <inheritdoc />
        public Guid InternalSessionId { get; } = Guid.NewGuid();

        internal Session(
            IEnumerable<object> contactPoints,
            Configuration configuration,
            string sessionName)
        {
            _metadata = new Metadata(configuration);

            var protocolVersion = Session.MaxProtocolVersion;
            if (Configuration.ProtocolOptions.MaxProtocolVersionValue != null &&
                Configuration.ProtocolOptions.MaxProtocolVersionValue.Value.IsSupported(configuration))
            {
                protocolVersion = Configuration.ProtocolOptions.MaxProtocolVersionValue.Value;
            }

            _protocolEventDebouncer = new ProtocolEventDebouncer(
                configuration.TimerFactory,
                TimeSpan.FromMilliseconds(configuration.MetadataSyncOptions.RefreshSchemaDelayIncrement),
                TimeSpan.FromMilliseconds(configuration.MetadataSyncOptions.MaxTotalRefreshSchemaDelay));

            var contactPointsList = contactPoints.ToList();
            if (contactPointsList.Count == 0)
            {
                Session.Logger.Info("No contact points provided, defaulting to {0}", Session.DefaultContactPoint);
                contactPointsList.Add(Session.DefaultContactPoint);
                _implicitContactPoint = true;
            }

            var parsedContactPoints = configuration.ContactPointParser.ParseContactPoints(contactPointsList);
            
            _controlConnection = configuration.ControlConnectionFactory.Create(
                this, 
                _protocolEventDebouncer, 
                protocolVersion, 
                Configuration, 
                _metadata, 
                parsedContactPoints);

            _metadata.ControlConnection = _controlConnection;

            _serializerManager = _controlConnection.SerializerManager;
            Configuration = configuration;
            Keyspace = configuration.ClientOptions.DefaultKeyspace;
            SessionName = sessionName;
            UserDefinedTypes = new UdtMappingDefinitions(this, _serializerManager);
            _connectionPool = new CopyOnWriteDictionary<IPEndPoint, IHostConnectionPool>();
            HostRemoved += OnHostRemoved;
            _metricsManager = new MetricsManager(configuration.MetricsProvider, Configuration.MetricsOptions, Configuration.MetricsEnabled, SessionName);
            _observerFactory = configuration.ObserverFactoryBuilder.Build(_metricsManager);
            _insightsClient = configuration.InsightsClientFactory.Create(this);
        }

        /// <inheritdoc />
        public Metadata Metadata => _metadata;

        /// <inheritdoc />
        public event Action<Host> HostAdded;

        /// <inheritdoc />
        public event Action<Host> HostRemoved;

        /// <inheritdoc />
        public IAsyncResult BeginExecute(IStatement statement, AsyncCallback callback, object state)
        {
            return ExecuteAsync(statement).ToApm(callback, state);
        }

        /// <inheritdoc />
        public IAsyncResult BeginExecute(string cqlQuery, ConsistencyLevel consistency, AsyncCallback callback, object state)
        {
            return BeginExecute(new SimpleStatement(cqlQuery).SetConsistencyLevel(consistency), callback, state);
        }

        /// <inheritdoc />
        public IAsyncResult BeginPrepare(string cqlQuery, AsyncCallback callback, object state)
        {
            return PrepareAsync(cqlQuery).ToApm(callback, state);
        }

        /// <inheritdoc />
        public void ChangeKeyspace(string keyspace)
        {
            if (Keyspace != keyspace)
            {
                Execute(new SimpleStatement(CqlQueryTools.GetUseKeyspaceCql(keyspace)));
                Keyspace = keyspace;
            }
        }

        /// <inheritdoc />
        public void CreateKeyspace(string keyspace, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            WaitForSchemaAgreement(Execute(CqlQueryTools.GetCreateKeyspaceCql(keyspace, replication, durableWrites, false)));
            Session.Logger.Info("Keyspace [" + keyspace + "] has been successfully CREATED.");
        }

        /// <inheritdoc />
        public void CreateKeyspaceIfNotExists(string keyspaceName, Dictionary<string, string> replication = null, bool durableWrites = true)
        {
            try
            {
                CreateKeyspace(keyspaceName, replication, durableWrites);
            }
            catch (AlreadyExistsException)
            {
                Session.Logger.Info(string.Format("Cannot CREATE keyspace:  {0}  because it already exists.", keyspaceName));
            }
        }

        /// <inheritdoc />
        public void DeleteKeyspace(string keyspaceName)
        {
            Execute(CqlQueryTools.GetDropKeyspaceCql(keyspaceName, false));
        }

        /// <inheritdoc />
        public void DeleteKeyspaceIfExists(string keyspaceName)
        {
            try
            {
                DeleteKeyspace(keyspaceName);
            }
            catch (InvalidQueryException)
            {
                Session.Logger.Info($"Cannot DELETE keyspace:  {keyspaceName}  because it not exists.");
            }
        }

        internal static Task<IInternalSession> BuildFromAsync(IInitializer initializer)
        {
            return Session.BuildFromAsync(initializer, null, null);
        }

        internal static Task<IInternalSession> BuildFromAsync(
            IInitializer initializer, IReadOnlyList<object> nonIpEndPointContactPoints)
        {
            return Session.BuildFromAsync(initializer, nonIpEndPointContactPoints, null);
        }

        internal static async Task<IInternalSession> BuildFromAsync(
            IInitializer initializer, 
            IReadOnlyList<object> nonIpEndPointContactPoints, 
            Configuration config)
        {
            config = config ?? initializer.GetConfiguration();
            nonIpEndPointContactPoints = nonIpEndPointContactPoints ?? new object[0];
            var newSessionName = Session.GetNewSessionName(config);
            var s = await config.SessionFactory.CreateSessionAsync(
                initializer.ContactPoints.Concat(nonIpEndPointContactPoints),
                config,
                newSessionName).ConfigureAwait(false);

            try
            {
                await s.Init().ConfigureAwait(false);
                return s;

            }
            catch (Exception)
            {
                await s.ShutdownAsync().ConfigureAwait(false);
                throw;
            }
        }

        private static string GetNewSessionName(Configuration config)
        {
            var sessionCounter = Session.GetAndIncrementSessionCounter();
            if (sessionCounter == 0 && config.SessionName != null)
            {
                return config.SessionName;
            }

            var prefix = config.SessionName ?? Configuration.DefaultSessionName;
            return prefix + sessionCounter;
        }

        private static long GetAndIncrementSessionCounter()
        {
            var newCounter = Interlocked.Increment(ref Session._sessionCounter);

            // Math.Abs just to avoid negative counters if it overflows
            return newCounter < 0 ? Math.Abs(newCounter) : newCounter;
        }

        /// <summary>
        ///  Creates a new Session Builder.
        /// </summary>
        public static Builder Builder()
        {
            return new Builder();
        }
        
        /// <inheritdoc />
        public async Task ShutdownAsync()
        {
            if (!_initialized)
            {
                return;
            }

            //Only dispose once
            if (Interlocked.Increment(ref _disposed) != 1)
            {
                return;
            }

            if (_insightsClient != null)
            {
                await _insightsClient.ShutdownAsync().ConfigureAwait(false);
            }

            _metricsManager?.Dispose();

            HostRemoved -= OnHostRemoved;

            var pools = _connectionPool.ToArray();
            foreach (var pool in pools)
            {
                pool.Value.Dispose();
            }

            _controlConnection.Dispose();
            await _protocolEventDebouncer.ShutdownAsync().ConfigureAwait(false);
            Configuration.Timer.Dispose();

            // Dispose policies
            var speculativeExecutionPolicies = new HashSet<ISpeculativeExecutionPolicy>(
                new ReferenceEqualityComparer<ISpeculativeExecutionPolicy>());
            foreach (var options in Configuration.RequestOptions.Values)
            {
                speculativeExecutionPolicies.Add(options.SpeculativeExecutionPolicy);
            }

            foreach (var sep in speculativeExecutionPolicies)
            {
                sep.Dispose();
            }

            Session.Logger.Info("Session [" + SessionName + "] has been shut down.");
        }

        /// <inheritdoc />
        public void Shutdown(int timeoutMs = Timeout.Infinite)
        {
            TaskHelper.WaitToComplete(Task.Run(ShutdownAsync), timeoutMs);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Shutdown();
        }
        
        /// <summary>
        /// Initializes once (Thread-safe) the control connection and metadata associated with the Cluster instance
        /// </summary>
        private async Task InitCluster()
        {
            if (_initialized)
            {
                //It was already initialized
                return;
            }

            await _initLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_initialized)
                {
                    //It was initialized when waiting on the lock
                    return;
                }

                if (_initException != null)
                {
                    //There was an exception that is not possible to recover from
                    throw _initException;
                }

                Session.Logger.Info("Connecting to cluster using {0}", Session.GetAssemblyInfo());
                try
                {
                    // Collect all policies in collections
                    var loadBalancingPolicies = new HashSet<ILoadBalancingPolicy>(new ReferenceEqualityComparer<ILoadBalancingPolicy>());
                    var speculativeExecutionPolicies = new HashSet<ISpeculativeExecutionPolicy>(new ReferenceEqualityComparer<ISpeculativeExecutionPolicy>());
                    foreach (var options in Configuration.RequestOptions.Values)
                    {
                        loadBalancingPolicies.Add(options.LoadBalancingPolicy);
                        speculativeExecutionPolicies.Add(options.SpeculativeExecutionPolicy);
                    }
                    
                    _loadBalancingPolicies = loadBalancingPolicies.ToList();

                    // Only abort the async operations when at least twice the time for ConnectTimeout per host passed
                    var initialAbortTimeout = Configuration.SocketOptions.ConnectTimeoutMillis * 2 * _metadata.Hosts.Count;
                    initialAbortTimeout = Math.Max(initialAbortTimeout, Configuration.SocketOptions.MetadataAbortTimeout);
                    var initTask = _controlConnection.InitAsync();
                    try
                    {
                        await initTask.WaitToCompleteAsync(initialAbortTimeout).ConfigureAwait(false);
                    }
                    catch (TimeoutException ex)
                    {
                        var newEx = new TimeoutException(
                            "Session initialization was aborted after timing out. This mechanism is put in place to" +
                            " avoid blocking the calling thread forever. This usually caused by a networking issue" +
                            " between the client driver instance and the cluster. You can increase this timeout via " +
                            "the SocketOptions.ConnectTimeoutMillis config setting. This can also be related to deadlocks " +
                            "caused by mixing synchronous and asynchronous code.", ex);
                        _initException = new InitFatalErrorException(newEx);
                        initTask.ContinueWith(t =>
                        {
                            if (t.IsFaulted && t.Exception != null)
                            {
                                _initException = new InitFatalErrorException(t.Exception.InnerException);
                            }
                        }, TaskContinuationOptions.ExecuteSynchronously).Forget();
                        throw newEx;
                    }

                    // initialize the local datacenter provider
                    Configuration.LocalDatacenterProvider.Initialize(this);
                    
                    // Initialize policies
                    foreach (var lbp in loadBalancingPolicies)
                    {
                        lbp.Initialize(this);
                    }

                    foreach (var sep in speculativeExecutionPolicies)
                    {
                        sep.Initialize(this);
                    }

                    InitializeHostDistances();

                    // Set metadata dependent options
                    SetMetadataDependentOptions();
                }
                catch (NoHostAvailableException)
                {
                    //No host available now, maybe later it can recover from
                    throw;
                }
                catch (TimeoutException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    //There was an error that the driver is not able to recover from
                    //Store the exception for the following times
                    _initException = new InitFatalErrorException(ex);
                    //Throw the actual exception for the first time
                    throw;
                }
                Session.Logger.Info(
                    "Session connected using binary protocol version: " +
                    $"[{_controlConnection.SerializerManager.CurrentProtocolVersion.GetStringRepresentation()}]");
                _initialized = true;
                _metadata.Hosts.Added += OnHostAdded;
                _metadata.Hosts.Removed += OnHostRemoved;
                _metadata.Hosts.Up += OnHostUp;
            }
            finally
            {
                _initLock.Release();
            }
        }

        private void OnHostRemoved(Host h)
        {
            _metricsManager.RemoveNodeMetrics(h);
            if (_connectionPool.TryRemove(h.Address, out var pool))
            {
                pool.OnHostRemoved();
                pool.Dispose();
            }

            HostRemoved?.Invoke(h);
        }

        private void OnHostAdded(Host h)
        {
            HostAdded?.Invoke(h);
        }

        private async void OnHostUp(Host h)
        {
            try
            {
                if (!Configuration.QueryOptions.IsReprepareOnUp())
                {
                    return;
                }

                // We should prepare all current queries on the host
                await ReprepareAllQueries(h).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Session.Logger.Error(
                    "An exception was thrown when preparing all queries on a host ({0}) " +
                    "that came UP:" + Environment.NewLine + "{1}", h?.Address?.ToString(), ex.ToString());
            }
        }
        
        private void InitializeHostDistances()
        {
            foreach (var host in AllHosts())
            {
                RetrieveAndSetDistance(host);
            }
        }

        private static string GetAssemblyInfo()
        {
            var assembly = typeof(ISession).GetTypeInfo().Assembly;
            var info = FileVersionInfo.GetVersionInfo(assembly.Location);
            return $"{info.ProductName} v{info.FileVersion}";
        }

        /// <inheritdoc />
        public ICollection<Host> AllHosts()
        {
            //Do not connect at first
            return _metadata.AllHosts();
        }

        private void SetMetadataDependentOptions()
        {
            if (_metadata.IsDbaas)
            {
                Configuration.SetDefaultConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }
        }

        /// <inheritdoc />
        public async Task Init()
        {
            await InitCluster().ConfigureAwait(false);

            _metricsManager.InitializeMetrics(this);

            if (Configuration.GetOrCreatePoolingOptions(_serializerManager.CurrentProtocolVersion).GetWarmup())
            {
                await Warmup().ConfigureAwait(false);
            }

            if (Keyspace != null)
            {
                // Borrow a connection, trying to fail fast
                var handler = Configuration.RequestHandlerFactory.Create(this, _serializerManager.GetCurrentSerializer());
                await handler.GetNextConnectionAsync(new Dictionary<IPEndPoint, Exception>()).ConfigureAwait(false);
            }
            
            _insightsClient.Init();
            
            Session.Logger.Info("Session [" + SessionName + "] has been initialized.");
        }

        public IReadOnlyDictionary<IContactPoint, IEnumerable<IConnectionEndPoint>> GetResolvedEndpoints()
        {
            return _metadata.ResolvedContactPoints;
        }

        /// <inheritdoc />
        public HostDistance RetrieveAndSetDistance(Host host)
        {
            var distance = _loadBalancingPolicies[0].Distance(host);

            for (var i = 1; i < _loadBalancingPolicies.Count; i++)
            {
                var lbp = _loadBalancingPolicies[i];
                var lbpDistance = lbp.Distance(host);
                if (lbpDistance < distance)
                {
                    distance = lbpDistance;
                }
            }

            host.SetDistance(distance);
            return distance;
        }
        
        /// <summary>
        /// Creates the required connections on all hosts in the local DC.
        /// Returns a Task that is marked as completed after all pools were warmed up.
        /// In case, all the host pool warmup fail, it logs an error.
        /// </summary>
        private async Task Warmup()
        {
            var hosts = AllHosts().Where(h => RetrieveAndSetDistance(h) == HostDistance.Local).ToArray();
            var tasks = new Task[hosts.Length];
            for (var i = 0; i < hosts.Length; i++)
            {
                var host = hosts[i];
                var pool = GetOrCreateConnectionPool(host, HostDistance.Local);
                tasks[i] = pool.Warmup();
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch
            {
                if (tasks.Any(t => t.Status == TaskStatus.RanToCompletion))
                {
                    // At least 1 of the warmup tasks completed
                    return;
                }

                // Log and continue as the ControlConnection is connected
                Session.Logger.Error($"Connection pools for {hosts.Length} host(s) failed to be warmed up");
            }
        }

        /// <inheritdoc />
        public RowSet EndExecute(IAsyncResult ar)
        {
            var task = (Task<RowSet>)ar;
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public PreparedStatement EndPrepare(IAsyncResult ar)
        {
            var task = (Task<PreparedStatement>)ar;
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement, string executionProfileName)
        {
            var task = ExecuteAsync(statement, executionProfileName);
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, Configuration.DefaultRequestOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public RowSet Execute(IStatement statement)
        {
            return Execute(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery)
        {
            return Execute(GetDefaultStatement(cqlQuery));
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, string executionProfileName)
        {
            return Execute(GetDefaultStatement(cqlQuery), executionProfileName);
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, ConsistencyLevel consistency)
        {
            return Execute(GetDefaultStatement(cqlQuery).SetConsistencyLevel(consistency));
        }

        /// <inheritdoc />
        public RowSet Execute(string cqlQuery, int pageSize)
        {
            return Execute(GetDefaultStatement(cqlQuery).SetPageSize(pageSize));
        }

        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement statement)
        {
            return ExecuteAsync(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public Task<RowSet> ExecuteAsync(IStatement statement, string executionProfileName)
        {
            return ExecuteAsync(statement, GetRequestOptions(executionProfileName));
        }

        private Task<RowSet> ExecuteAsync(IStatement statement, IRequestOptions requestOptions)
        {
            return Configuration.RequestHandlerFactory
                                .Create(this, _serializerManager.GetCurrentSerializer(), statement, requestOptions)
                                .SendAsync();
        }

        /// <inheritdoc />
        public IHostConnectionPool GetOrCreateConnectionPool(Host host, HostDistance distance)
        {
            var hostPool = _connectionPool.GetOrAdd(host.Address, address =>
            {
                var newPool = Configuration.HostConnectionPoolFactory.Create(
                    host, Configuration, _serializerManager, _observerFactory);
                newPool.AllConnectionClosed += OnAllConnectionClosed;
                newPool.SetDistance(distance);
                _metricsManager.GetOrCreateNodeMetrics(host).InitializePoolGauges(newPool);
                return newPool;
            });
            return hostPool;
        }

        /// <inheritdoc />
        public IEnumerable<KeyValuePair<IPEndPoint, IHostConnectionPool>> GetPools()
        {
            return _connectionPool.Select(kvp => new KeyValuePair<IPEndPoint, IHostConnectionPool>(kvp.Key, kvp.Value));
        }

        public bool AnyOpenConnections(Host host)
        {
            return HasConnections(host);
        }

        public void OnAllConnectionClosed(Host host, IHostConnectionPool pool)
        {
            if (AnyOpenConnections(host))
            {
                pool.ScheduleReconnection();
                return;
            }

            // There isn't any open connection to this host in any of the pools
            pool.MarkAsDownAndScheduleReconnection();
        }

        /// <inheritdoc/>
        public int ConnectedNodes => _connectionPool.Count(kvp => kvp.Value.HasConnections);

        public IDriverMetrics GetMetrics()
        {
            return _metricsManager;
        }

        public bool HasConnections(Host host)
        {
            if (_connectionPool.TryGetValue(host.Address, out var pool))
            {
                return pool.HasConnections;
            }
            return false;
        }

        /// <inheritdoc />
        public IHostConnectionPool GetExistingPool(IPEndPoint address)
        {
            _connectionPool.TryGetValue(address, out var pool);
            return pool;
        }

        public void CheckHealth(Host host, IConnection connection)
        {
            if (!_connectionPool.TryGetValue(host.Address, out var pool))
            {
                Session.Logger.Error("Internal error: No host connection pool found");
                return;
            }
            pool.CheckHealth(connection);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery)
        {
            return Prepare(cqlQuery, null, null);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, IDictionary<string, byte[]> customPayload)
        {
            return Prepare(cqlQuery, null, customPayload);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, string keyspace)
        {
            return Prepare(cqlQuery, keyspace, null);
        }

        /// <inheritdoc />
        public PreparedStatement Prepare(string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            var task = PrepareAsync(cqlQuery, keyspace, customPayload);
            TaskHelper.WaitToCompleteWithMetrics(_metricsManager, task, Configuration.ClientOptions.QueryAbortTimeout);
            return task.Result;
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query)
        {
            return PrepareAsync(query, null, null);
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string query, IDictionary<string, byte[]> customPayload)
        {
            return PrepareAsync(query, null, customPayload);
        }

        /// <inheritdoc />
        public Task<PreparedStatement> PrepareAsync(string cqlQuery, string keyspace)
        {
            return PrepareAsync(cqlQuery, keyspace, null);
        }

        /// <inheritdoc />
        public async Task<PreparedStatement> PrepareAsync(
            string cqlQuery, string keyspace, IDictionary<string, byte[]> customPayload)
        {
            var serializer = _serializerManager.GetCurrentSerializer();
            var currentVersion = serializer.ProtocolVersion;
            if (!currentVersion.SupportsKeyspaceInRequest() && keyspace != null)
            {
                // Validate protocol version here and not at PrepareRequest level, as PrepareRequest can be issued
                // in the background (prepare and retry, prepare on up, ...)
                throw new NotSupportedException($"Protocol version {currentVersion} does not support" +
                                                " setting the keyspace as part of the PREPARE request");
            }
            var request = new PrepareRequest(serializer, cqlQuery, keyspace, customPayload);
            return await PrepareInternal(_serializerManager, request).ConfigureAwait(false);
        }

        public void WaitForSchemaAgreement(RowSet rs)
        {
        }

        public bool WaitForSchemaAgreement(IPEndPoint hostAddress)
        {
            return false;
        }

        private IStatement GetDefaultStatement(string cqlQuery)
        {
            return new SimpleStatement(cqlQuery);
        }

        /// <inheritdoc />
        public IRequestOptions GetRequestOptions(string executionProfileName)
        {
            if (!Configuration.RequestOptions.TryGetValue(executionProfileName, out var profile))
            {
                throw new ArgumentException("The provided execution profile name does not exist. It must be added through the Session Builder.");
            }

            return profile;
        }
        
        /// <inheritdoc />
        public GraphResultSet ExecuteGraph(IGraphStatement statement)
        {
            return ExecuteGraph(statement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement)
        {
            return ExecuteGraphAsync(graphStatement, Configuration.DefaultExecutionProfileName);
        }

        /// <inheritdoc />
        public GraphResultSet ExecuteGraph(IGraphStatement statement, string executionProfileName)
        {
            return TaskHelper.WaitToCompleteWithMetrics(_metricsManager, ExecuteGraphAsync(statement, executionProfileName));
        }

        /// <inheritdoc />
        public async Task<GraphResultSet> ExecuteGraphAsync(IGraphStatement graphStatement, string executionProfileName)
        {
            var requestOptions = GetRequestOptions(executionProfileName);
            var stmt = graphStatement.ToIStatement(requestOptions.GraphOptions);
            await GetAnalyticsMaster(stmt, graphStatement, requestOptions).ConfigureAwait(false);
            var rs = await ExecuteAsync(stmt, requestOptions).ConfigureAwait(false);
            return GraphResultSet.CreateNew(rs, graphStatement, requestOptions.GraphOptions);
        }

        /// <inheritdoc />
        public Host GetHost(IPEndPoint address)
        {
            return Metadata.GetHost(address);
        }

        /// <inheritdoc />
        public ICollection<Host> GetReplicas(byte[] partitionKey)
        {
            return Metadata.GetReplicas(partitionKey);
        }

        /// <inheritdoc />
        public ICollection<Host> GetReplicas(string keyspace, byte[] partitionKey)
        {
            return Metadata.GetReplicas(keyspace, partitionKey);
        }

        /// <inheritdoc />
        public async Task<PreparedStatement> PrepareInternal(
            ISerializerManager serializerManager, PrepareRequest request)
        {
            var lbp = Configuration.DefaultRequestOptions.LoadBalancingPolicy;
            var handler = Configuration.PrepareHandlerFactory.CreatePrepareHandler(serializerManager, this);
            var ps = await handler.Prepare(request, this, lbp.NewQueryPlan(Keyspace, null).GetEnumerator()).ConfigureAwait(false);
            var psAdded = PreparedQueries.GetOrAdd(ps.Id, ps);
            if (ps != psAdded)
            {
                PrepareHandler.Logger.Warning("Re-preparing already prepared query is generally an anti-pattern and will likely " +
                                              "affect performance. Consider preparing the statement only once. Query='{0}'", ps.Cql);
                ps = psAdded;
            }

            return ps;
        }

        /// <inheritdoc />
        public bool RefreshSchema(string keyspace = null, string table = null)
        {
            return Metadata.RefreshSchema(keyspace, table);
        }

        /// <inheritdoc />
        public Task<bool> RefreshSchemaAsync(string keyspace = null, string table = null)
        {
            return Metadata.RefreshSchemaAsync(keyspace, table);
        }

        /// <inheritdoc />
        public ConcurrentDictionary<byte[], PreparedStatement> PreparedQueries { get; }
            = new ConcurrentDictionary<byte[], PreparedStatement>(new ByteArrayComparer());

        private async Task ReprepareAllQueries(Host host)
        {
            ICollection<PreparedStatement> preparedQueries = PreparedQueries.Values;

            if (preparedQueries.Count == 0)
            {
                return;
            }
            
            // Get the first pool for that host that has open connections
            var pool = GetExistingPool(host.Address);
            if (pool == null || !pool.HasConnections)
            {
                PrepareHandler.Logger.Info($"Not re-preparing queries on {host.Address} as there wasn't an open connection to the node.");
                return;
            }

            PrepareHandler.Logger.Info($"Re-preparing {preparedQueries.Count} queries on {host.Address}");
            var tasks = new List<Task>(preparedQueries.Count);
            var handler = Configuration.PrepareHandlerFactory.CreateReprepareHandler();
            var serializer = _metadata.ControlConnection.SerializerManager.GetCurrentSerializer();
            using (var semaphore = new SemaphoreSlim(64, 64))
            {
                foreach (var ps in preparedQueries)
                {
                    var request = new PrepareRequest(serializer, ps.Cql, ps.Keyspace, null);
                    await semaphore.WaitAsync().ConfigureAwait(false);
                    tasks.Add(Task.Run(() => handler.ReprepareOnSingleNodeAsync(
                        new KeyValuePair<Host, IHostConnectionPool>(host, pool), 
                        ps, 
                        request, 
                        semaphore, 
                        true)));
                }

                try
                {
                    await Task.WhenAll(tasks).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    PrepareHandler.Logger.Info(
                        "There was an error when re-preparing queries on {0}. " +
                        "The driver will re-prepare the queries individually the next time they are sent to this node. " +
                        "Exception: {1}",
                        host.Address,
                        ex);
                }
            }
        }

        private async Task<IStatement> GetAnalyticsMaster(
            IStatement statement, IGraphStatement graphStatement, IRequestOptions requestOptions)
        {
            if (!(statement is TargettedSimpleStatement) || !requestOptions.GraphOptions.IsAnalyticsQuery(graphStatement))
            {
                return statement;
            }

            var targetedSimpleStatement = (TargettedSimpleStatement)statement;

            RowSet rs;
            try
            {
                rs = await ExecuteAsync(
                    new SimpleStatement("CALL DseClientTool.getAnalyticsGraphServer()"), requestOptions).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Session.Logger.Verbose("Error querying graph analytics server, query will not be routed optimally: {0}", ex);
                return statement;
            }

            return AdaptRpcMasterResult(rs, targetedSimpleStatement);
        }

        private IStatement AdaptRpcMasterResult(RowSet rowSet, TargettedSimpleStatement statement)
        {
            var row = rowSet.FirstOrDefault();
            if (row == null)
            {
                Session.Logger.Verbose("Empty response querying graph analytics server, query will not be routed optimally");
                return statement;
            }
            var resultField = row.GetValue<IDictionary<string, string>>("result");
            if (resultField == null || !resultField.ContainsKey("location") || resultField["location"] == null)
            {
                Session.Logger.Verbose("Could not extract graph analytics server location from RPC, query will not be routed optimally");
                return statement;
            }
            var location = resultField["location"];
            var hostName = location.Substring(0, location.LastIndexOf(':'));
            var address = Configuration.AddressTranslator.Translate(
                new IPEndPoint(IPAddress.Parse(hostName), Configuration.ProtocolOptions.Port));
            var host = GetHost(address);
            statement.PreferredHost = host;
            return statement;
        }
    }
}