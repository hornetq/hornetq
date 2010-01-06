/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.api.jms.management;

import java.util.Map;

import javax.management.MBeanOperationInfo;

import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.config.TransportConfiguration;
import org.hornetq.api.core.management.Operation;
import org.hornetq.api.core.management.Parameter;
import org.hornetq.spi.core.remoting.ConnectorFactory;

/**
 * A JMSSserverControl is used to manage HornetQ JMS server.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface JMSServerControl
{
   // Attributes ----------------------------------------------------

   /**
    * Returns whether this server is started.
    */
   boolean isStarted();

   /**
    * Returns this server's version
    */
   String getVersion();

   /**
    * Returns the names of the JMS topics available on this server.
    */
   String[] getTopicNames();

   /**
    * Returns the names of the JMS queues available on this server.
    */
   String[] getQueueNames();

   /**
    * Returns the names of the JMS connection factories available on this server.
    */
   String[] getConnectionFactoryNames();

   // Operations ----------------------------------------------------

   /**
    * Creates a JMS Queue with the specified name and JNDI binding.
    * 
    * @return {@code true} if the queue was created, {@code false} else
    */
   @Operation(desc = "Create a JMS Queue", impact = MBeanOperationInfo.ACTION)
   boolean createQueue(@Parameter(name = "name", desc = "Name of the queue to create") String name,
                       @Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI") String jndiBinding) throws Exception;

   /**
    * Destroys a JMS Queue with the specified name.
    * 
    * @return {@code true} if the queue was destroyed, {@code false} else
    */
   @Operation(desc = "Destroy a JMS Queue", impact = MBeanOperationInfo.ACTION)
   boolean destroyQueue(@Parameter(name = "name", desc = "Name of the queue to destroy") String name) throws Exception;

   /**
    * Creates a JMS Topic with the specified name and JNDI binding.
    * 
    * @return {@code true} if the topic was created, {@code false} else
    */
   @Operation(desc = "Create a JMS Topic", impact = MBeanOperationInfo.ACTION)
   boolean createTopic(@Parameter(name = "name", desc = "Name of the topic to create") String name,
                       @Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI") String jndiBinding) throws Exception;

   /**
    * Destroys a JMS Topic with the specified name.
    * 
    * @return {@code true} if the topic was destroyed, {@code false} else
    */
   @Operation(desc = "Destroy a JMS Topic", impact = MBeanOperationInfo.ACTION)
   boolean destroyTopic(@Parameter(name = "name", desc = "Name of the topic to destroy") String name) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a static list of live-backup servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * {@code liveConnectorsTransportClassNames} (resp. {@code backupConnectorsTransportClassNames}) are the class names 
    * of the {@link ConnectorFactory} to connect to the live (resp. backup) servers
    * and {@code liveConnectorTransportParams} (resp. backupConnectorTransportParams) are Map&lt;String, Object&gt; for the corresponding {@link TransportConfiguration}'s parameters.
    * 
    * @see ClientSessionFactory#setStaticConnectors(java.util.List)
    */
   void createConnectionFactory(String name,
                                Object[] liveConnectorsTransportClassNames,
                                Object[] liveConnectorTransportParams,
                                Object[] backupConnectorsTransportClassNames,
                                Object[] backupConnectorTransportParams,
                                Object[] bindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single live-backup pair of servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * {@code backupTransportClassNames} and {@code backupTransportParams} can be {@code null} if there is no backup server.
    */
   @Operation(desc = "Create a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "liveTransportClassNames", desc = "comma-separated list of class names for transport to live servers") String liveTransportClassNames,
                                @Parameter(name = "liveTransportParams", desc = "comma-separated list of key=value parameters for the live transports (enclosed between { } for each transport)") String liveTransportParams,
                                @Parameter(name = "backupTransportClassNames", desc = "comma-separated list of class names for transport to backup servers") String backupTransportClassNames,
                                @Parameter(name = "backupTransportParams", desc = "comma-separated list of key=value parameters for the backup transports (enclosed between { } for each transport)") String backupTransportParams,
                                @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings") String jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a static list of live-backup servers.
    * <br>
    * Connections created by this ConnectionFactory will have their ClientID set to the specified ClientID.
    * 
    * @see #createConnectionFactory(String, Object[], Object[], Object[], Object[], Object[])
    */
   void createConnectionFactory(String name,
                                Object[] liveConnectorsTransportClassNames,
                                Object[] liveConnectorTransportParams,
                                Object[] backupConnectorsTransportClassNames,
                                Object[] backupConnectorTransportParams,
                                String clientID,
                                Object[] jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single live-backup pair of servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * Connections created by this ConnectionFactory will have their ClientID set to the specified ClientID.
    * <br>
    * {@code backupTransportClassNames} and {@code backupTransportParams} can be {@code null} if there is no backup server.
    */
   @Operation(desc = "Create a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "liveTransportClassNames", desc = "comma-separated list of class names for transport to live servers") String liveTransportClassNames,
                                @Parameter(name = "liveTransportParams", desc = "comma-separated list of key=value parameters for the live transports (enclosed between { } for each transport)") String liveTransportParams,
                                @Parameter(name = "backupTransportClassNames", desc = "comma-separated list of class names for transport to backup servers") String backupTransportClassNames,
                                @Parameter(name = "backupTransportParams", desc = "comma-separated list of key=value parameters for the backup transports (enclosed between { } for each transport)") String backupTransportParams,
                                @Parameter(name = "clientID") String clientID,
                                @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings") String jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a static list of live-backup servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * All parameters corresponds to the underlying ClientSessionFactory used by the factory.
    * 
    * @see #createConnectionFactory(String, Object[], Object[], Object[], Object[], Object[])
    * @see ClientSessionFactory
    */
   void createConnectionFactory(String name,
                                Object[] liveConnectorsTransportClassNames,
                                Object[] liveConnectorTransportParams,
                                Object[] backupConnectorsTransportClassNames,
                                Object[] backupConnectorTransportParams,
                                String clientID,
                                long clientFailureCheckPeriod,
                                long connectionTTL,
                                long callTimeout,
                                boolean cacheLargeMessageClient,
                                int minLargeMessageSize,
                                int consumerWindowSize,
                                int consumerMaxRate,
                                int confirmationWindowSize,
                                int producerWindowSize,
                                int producerMaxRate,
                                boolean blockOnAcknowledge,
                                boolean blockOnDurableSend,
                                boolean blockOnNonDurableSend,
                                boolean autoGroup,
                                boolean preAcknowledge,
                                String loadBalancingPolicyClassName,
                                int transactionBatchSize,
                                int dupsOKBatchSize,
                                boolean useGlobalPools,
                                int scheduledThreadPoolMaxSize,
                                int threadPoolMaxSize,
                                long retryInterval,
                                double retryIntervalMultiplier,
                                long maxRetryInterval,
                                int reconnectAttempts,
                                boolean failoverOnServerShutdown,
                                String groupID,
                                Object[] jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single live-backup pair of servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * All parameters corresponds to the underlying ClientSessionFactory used by the factory.
    * 
    * @see #createConnectionFactory(String, Object[], Object[], Object[], Object[], Object[])
    * @see ClientSessionFactory
    */
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "liveTransportClassNames", desc = "comma-separated list of class names for transport to live servers") String liveTransportClassNames,
                                @Parameter(name = "liveTransportParams", desc = "comma-separated list of key=value parameters for the live transports (enclosed between { } for each transport)") String liveTransportParams,
                                @Parameter(name = "backupTransportClassNames", desc = "comma-separated list of class names for transport to backup servers") String backupTransportClassNames,
                                @Parameter(name = "backupTransportParams", desc = "comma-separated list of key=value parameters for the backup transports (enclosed between { } for each transport)") String backupTransportParams,
                                @Parameter(name = "clientID") String clientID,
                                @Parameter(name = "clientFailureCheckPeriod") long clientFailureCheckPeriod,
                                @Parameter(name = "connectionTTL") long connectionTTL,
                                @Parameter(name = "callTimeout") long callTimeout,
                                @Parameter(name = "cacheLargemessageClient") boolean cacheLargeMessageClient,
                                @Parameter(name = "minLargeMessageSize") int minLargeMessageSize,
                                @Parameter(name = "consumerWindowSize") int consumerWindowSize,
                                @Parameter(name = "consumerMaxRate") int consumerMaxRate,
                                @Parameter(name = "confirmationWindowSize") int confirmationWindowSize,
                                @Parameter(name = "producerWindowSize") int producerWindowSize,
                                @Parameter(name = "producerMaxRate") int producerMaxRate,
                                @Parameter(name = "blockOnAcknowledge") boolean blockOnAcknowledge,
                                @Parameter(name = "blockOnDurableSend") boolean blockOnDurableSend,
                                @Parameter(name = "blockOnNonDurableSend") boolean blockOnNonDurableSend,
                                @Parameter(name = "autoGroup") boolean autoGroup,
                                @Parameter(name = "preAcknowledge") boolean preAcknowledge,
                                @Parameter(name = "loadBalancingPolicyClassName") String loadBalancingPolicyClassName,
                                @Parameter(name = "transactionBatchSize") int transactionBatchSize,
                                @Parameter(name = "dupsOKBatchSize") int dupsOKBatchSize,
                                @Parameter(name = "useGlobalPools") boolean useGlobalPools,
                                @Parameter(name = "scheduledThreadPoolMaxSize") int scheduledThreadPoolMaxSize,
                                @Parameter(name = "threadPoolMaxSize") int threadPoolMaxSize,
                                @Parameter(name = "retryInterval") long retryInterval,
                                @Parameter(name = "retryIntervalMultiplier") double retryIntervalMultiplier,
                                @Parameter(name = "maxRetryInterval") long maxRetryInterval,
                                @Parameter(name = "reconnectAttempts") int reconnectAttempts,
                                @Parameter(name = "failoverOnServerShutdown") boolean failoverOnServerShutdown,
                                @Parameter(name = "groupID") String groupID,
                                @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings") String jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name using a discovery group to discover HornetQ servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * This factory listens to the specified {@code discoveryAddress} and {@code discoveryPort} to discover which servers it can connect to.
    * <br>
    * Connections created by this ConnectionFactory will have their ClientID set to the specified ClientID.
    * 
    * @see #createConnectionFactory(String, Object[], Object[], Object[], Object[], Object[])
    */
   void createConnectionFactory(String name,
                                String discoveryAddress,
                                int discoveryPort,
                                String clientID,
                                Object[] bindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name using a discovery group to discover HornetQ servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for the specified bindings Strings
    * <br>
    * This factory listens to the specified {@code discoveryAddress} and {@code discoveryPort} to discover which servers it can connect to.
    * <br>
    * Connections created by this ConnectionFactory will have their ClientID set to the specified ClientID.
    * 
    * @see #createConnectionFactory(String, Object[], Object[], Object[], Object[], Object[])
    */
   @Operation(desc = "Create a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "discoveryAddress") String discoveryAddress,
                                @Parameter(name = "discoveryPort") int discoveryPort,
                                @Parameter(name = "clientID") String clientID,
                                @Parameter(name = "jndiBindings") String jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name using a discovery group to discover HornetQ servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * This factory listens to the specified {@code discoveryAddress} and {@code discoveryPort} to discover which servers it can connect to.
    * <br>
    * All parameters corresponds to the underlying ClientSessionFactory used by the factory.
    * 
    * @see ClientSessionFactory
    */
   void createConnectionFactory(String name,
                                String discoveryAddress,
                                int discoveryPort,
                                String clientID,
                                long discoveryRefreshTimeout,
                                long clientFailureCheckPeriod,
                                long connectionTTL,
                                long callTimeout,
                                boolean cacheLargeMessageClient,
                                int minLargeMessageSize,
                                int consumerWindowSize,
                                int consumerMaxRate,
                                int confirmationWindowSize,
                                int producerWindowSize,
                                int producerMaxRate,
                                boolean blockOnAcknowledge,
                                boolean blockOnDurableSend,
                                boolean blockOnNonDurableSend,
                                boolean autoGroup,
                                boolean preAcknowledge,
                                String loadBalancingPolicyClassName,
                                int transactionBatchSize,
                                int dupsOKBatchSize,
                                long initialWaitTimeout,
                                boolean useGlobalPools,
                                int scheduledThreadPoolMaxSize,
                                int threadPoolMaxSize,
                                long retryInterval,
                                double retryIntervalMultiplier,
                                long maxRetryInterval,
                                int reconnectAttempts,
                                boolean failoverOnServerShutdown,
                                String groupID,
                                Object[] jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name using a discovery group to discover HornetQ servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified comma-separated bindings.
    * <br>
    * This factory listens to the specified {@code discoveryAddress} and {@code discoveryPort} to discover which servers it can connect to.
    * <br>
    * All parameters corresponds to the underlying ClientSessionFactory used by the factory.
    * 
    * @see ClientSessionFactory
    */
   @Operation(desc = "Create a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "discoveryAddress") String discoveryAddress,
                                @Parameter(name = "discoveryPort") int discoveryPort,
                                @Parameter(name = "clientID") String clientID,
                                @Parameter(name = "discoveryRefreshTimeout") long discoveryRefreshTimeout,
                                @Parameter(name = "clientFailureCheckPeriod") long clientFailureCheckPeriod,
                                @Parameter(name = "connectionTTL") long connectionTTL,
                                @Parameter(name = "callTimeout") long callTimeout,
                                @Parameter(name = "cacheLargemessageClient") boolean cacheLargeMessageClient,
                                @Parameter(name = "minLargeMessageSize") int minLargeMessageSize,
                                @Parameter(name = "consumerWindowSize") int consumerWindowSize,
                                @Parameter(name = "consumerMaxRate") int consumerMaxRate,
                                @Parameter(name = "confirmationWindowSize") int confirmationWindowSize,
                                @Parameter(name = "producerWindowSize") int producerWindowSize,
                                @Parameter(name = "producerMaxRate") int producerMaxRate,
                                @Parameter(name = "blockOnAcknowledge") boolean blockOnAcknowledge,
                                @Parameter(name = "blockOnDurableSend") boolean blockOnDurableSend,
                                @Parameter(name = "blockOnNonDurableSend") boolean blockOnNonDurableSend,
                                @Parameter(name = "autoGroup") boolean autoGroup,
                                @Parameter(name = "preAcknowledge") boolean preAcknowledge,
                                @Parameter(name = "loadBalancingPolicyClassName") String loadBalancingPolicyClassName,
                                @Parameter(name = "transactionBatchSize") int transactionBatchSize,
                                @Parameter(name = "dupsOKBatchSize") int dupsOKBatchSize,
                                @Parameter(name = "initialWaitTimeout") long initialWaitTimeout,
                                @Parameter(name = "useGlobalPools") boolean useGlobalPools,
                                @Parameter(name = "scheduledThreadPoolMaxSize") int scheduledThreadPoolMaxSize,
                                @Parameter(name = "threadPoolMaxSize") int threadPoolMaxSize,
                                @Parameter(name = "retryInterval") long retryInterval,
                                @Parameter(name = "retryIntervalMultiplier") double retryIntervalMultiplier,
                                @Parameter(name = "maxRetryInterval") long maxRetryInterval,
                                @Parameter(name = "reconnectAttempts") int reconnectAttempts,
                                @Parameter(name = "failoverOnServerShutdown") boolean failoverOnServerShutdown,
                                @Parameter(name = "groupID") String groupID,
                                @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings") String jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single HornetQ server.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings.
    */
   void createConnectionFactory(String name,
                                String liveTransportClassName,
                                Map<String, Object> liveTransportParams,
                                Object[] jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single HornetQ server.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified comma-separated bindings.
    * <br>
    * The {@code liveTransportParams} is a  comma-separated list of key=value for the transport parameters corresponding to the {@code TransportConfiguration} parameters.
    */
   @Operation(desc = "Create a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "liveTransportClassName") String liveTransportClassName,
                                @Parameter(name = "liveTransportParams", desc = "comma-separated list of key=value for the transport parameters") String liveTransportParams,
                                @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings") String jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single HornetQ server.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified comma-separated bindings.
    * <br>
    * Connections created by this ConnectionFactory will have their ClientID set to the specified ClientID.
    */
   void createConnectionFactory(String name,
                                String liveTransportClassName,
                                Map<String, Object> liveTransportParams,
                                String clientID,
                                Object[] jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single HornetQ server.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified comma-separated bindings.
    * <br>
    * The {@code liveTransportParams} is a  comma-separated list of key=value for the transport parameters corresponding to the {@code TransportConfiguration} parameters.
    * <br>
    * Connections created by this ConnectionFactory will have their ClientID set to the specified ClientID.
    */
   @Operation(desc = "Create a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "liveTransportClassName") String liveTransportClassName,
                                @Parameter(name = "liveTransportParams", desc = "comma-separated list of key=value for the transport parameters") String liveTransportParams,
                                @Parameter(name = "clientID") String clientID,
                                @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings") String jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a static list of live-backup HornetQ servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified comma-separated bindings.
    */
   void createConnectionFactory(String name,
                                String liveTransportClassName,
                                Map<String, Object> liveTransportParams,
                                String backupTransportClassName,
                                Map<String, Object> backupTransportParams,
                                Object[] jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a static list of live-backup HornetQ servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified comma-separated bindings.
    * <br>
    * Connections created by this ConnectionFactory will have their ClientID set to the specified ClientID.
    */
   void createConnectionFactory(String name,
                                String liveTransportClassName,
                                Map<String, Object> liveTransportParams,
                                String backupTransportClassName,
                                Map<String, Object> backupTransportParams,
                                String clientID,
                                Object[] jndiBindings) throws Exception;

   /**
    * Destroy the ConnectionFactory corresponding to the specified name.
    */
   @Operation(desc = "Destroy a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void destroyConnectionFactory(@Parameter(name = "name", desc = "Name of the ConnectionFactory to destroy") String name) throws Exception;

   /**
    * Lists the addresses of all the clients connected to this address.
    */
   @Operation(desc = "List the client addresses", impact = MBeanOperationInfo.INFO)
   String[] listRemoteAddresses() throws Exception;

   /**
    * Lists the addresses of the clients connected to this address which matches the specified IP address.
    */
   @Operation(desc = "List the client addresses which match the given IP Address", impact = MBeanOperationInfo.INFO)
   String[] listRemoteAddresses(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   /**
    * Closes all the connections of clients connected to this server which matches the specified IP address.
    */
   @Operation(desc = "Closes all the connections for the given IP Address", impact = MBeanOperationInfo.INFO)
   boolean closeConnectionsForAddress(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   /**
    * Lists all the IDs of the connections connected to this server.
    */
   @Operation(desc = "List all the connection IDs", impact = MBeanOperationInfo.INFO)
   String[] listConnectionIDs() throws Exception;

   /**
    * Lists all the sessions IDs for the specified connection ID.
    */
   @Operation(desc = "List the sessions for the given connectionID", impact = MBeanOperationInfo.INFO)
   String[] listSessions(@Parameter(desc = "a connection ID", name = "connectionID") String connectionID) throws Exception;
}
