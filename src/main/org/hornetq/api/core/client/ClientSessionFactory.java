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

package org.hornetq.api.core.client;

import java.util.List;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.loadbalance.ConnectionLoadBalancingPolicy;

/**
 * A ClientSessionFactory is the entry point to create and configure HornetQ resources to produce and consume messages.
 * <br>
 * It is possible to configure a factory using the setter methods only if no session has been created.
 * Once a session is created, the configuration is fixed and any call to a setter method will throw a IllegalStateException.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientSessionFactory
{
   /**
    * Creates a session with XA transaction semantics.
    * 
    * @return a ClientSession with XA transaction semantics
    * 
    * @throws HornetQException if an exception occurs while creating the session
    */
   ClientSession createXASession() throws HornetQException;

   /**
    * Creates a <em>transacted</em> session.
    * 
    * It is up to the client to commit when sending and acknowledging messages.

    * @return a transacted ClientSession
    * @throws HornetQException if an exception occurs while creating the session
    * 
    * @see ClientSession#commit()
    */
   ClientSession createTransactedSession() throws HornetQException;


   /**
    * Creates a <em>non-transacted</em> session.
    * Message sends and acknowledgements are automatically committed by the session. <em>This does not
    * mean that messages are automatically acknowledged</em>, only that when messages are acknowledged, 
    * the session will automatically commit the transaction containing the acknowledgements.

    * @return a non-transacted ClientSession
    * @throws HornetQException if an exception occurs while creating the session
    */
   ClientSession createSession() throws HornetQException;

   /**
    * Creates a session.
    * 
    * @param autoCommitSends <code>true</code> to automatically commit message sends, <code>false</code> to commit manually
    * @param autoCommitAcks <code>true</code> to automatically commit message acknowledgement, <code>false</code> to commit manually
    * @return a ClientSession
    * @throws HornetQException if an exception occurs while creating the session
    */
   ClientSession createSession(boolean autoCommitSends, boolean autoCommitAcks) throws HornetQException;

   /**
    * Creates a session.
    * 
    * @param autoCommitSends <code>true</code> to automatically commit message sends, <code>false</code> to commit manually
    * @param autoCommitAcks <code>true</code> to automatically commit message acknowledgement, <code>false</code> to commit manually
    * @param ackBatchSize the batch size of the acknowledgements
    * @return a ClientSession
    * @throws HornetQException if an exception occurs while creating the session
    */
   ClientSession createSession(boolean autoCommitSends, boolean autoCommitAcks, int ackBatchSize) throws HornetQException;

   /**
    * Creates a session.
    * 
    * @param xa whether the session support XA transaction semantic or not
    * @param autoCommitSends <code>true</code> to automatically commit message sends, <code>false</code> to commit manually
    * @param autoCommitAcks <code>true</code> to automatically commit message acknowledgement, <code>false</code> to commit manually
    * @return a ClientSession
    * @throws HornetQException if an exception occurs while creating the session
    */
   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks) throws HornetQException;

   /**
    * Creates a session.
    * 
    * It is possible to <em>pre-acknowledge messages on the server</em> so that the client can avoid additional network trip
    * to the server to acknowledge messages. While this increase performance, this does not guarantee delivery (as messages
    * can be lost after being pre-acknowledged on the server). Use with caution if your application design permits it.
    * 
    * @param xa whether the session support XA transaction semantic or not
    * @param autoCommitSends <code>true</code> to automatically commit message sends, <code>false</code> to commit manually
    * @param autoCommitAcks <code>true</code> to automatically commit message acknowledgement, <code>false</code> to commit manually
    * @param preAcknowledge <code>true</code> to pre-acknowledge messages on the server, <code>false</code> to let the client acknowledge the messages
    * @return a ClientSession
    * @throws HornetQException if an exception occurs while creating the session
    */
   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge) throws HornetQException;

   /**
    * Creates an <em>authenticated</em> session.
    * 
    * It is possible to <em>pre-acknowledge messages on the server</em> so that the client can avoid additional network trip
    * to the server to acknowledge messages. While this increase performance, this does not guarantee delivery (as messages
    * can be lost after being pre-acknowledged on the server). Use with caution if your application design permits it.
    * 
    * @param username the user name
    * @param password the user password
    * @param xa whether the session support XA transaction semantic or not
    * @param autoCommitSends <code>true</code> to automatically commit message sends, <code>false</code> to commit manually
    * @param autoCommitAcks <code>true</code> to automatically commit message acknowledgement, <code>false</code> to commit manually
    * @param preAcknowledge <code>true</code> to pre-acknowledge messages on the server, <code>false</code> to let the client acknowledge the messages
    * @return a ClientSession
    * @throws HornetQException if an exception occurs while creating the session
    */
   ClientSession createSession(String username,
                               String password,
                               boolean xa,
                               boolean autoCommitSends,
                               boolean autoCommitAcks,
                               boolean preAcknowledge,
                               int ackBatchSize) throws HornetQException;

   /**
    * Returns the list of <em>live - backup</em> connectors pairs configured 
    * that sessions created by this factory will use to connect
    * to HornetQ servers or <code>null</code> if the factory is using discovery group.
    * 
    * The backup configuration (returned by {@link org.hornetq.api.core.Pair#b}) can be <code>null</code> if there is no
    * backup for the corresponding live configuration (returned by {@link org.hornetq.api.core.Pair#a})
    * 
    * @return a list of pair of TransportConfiguration corresponding to the live - backup nodes
    */
   List<Pair<TransportConfiguration, TransportConfiguration>> getStaticConnectors();

   /**
    * Sets the static list of live - backup connectors pairs that sessions created by this factory will use to connect
    * to HornetQ servers.
    * 
    * The backup configuration (returned by {@link Pair#b}) can be <code>null</code> if there is no
    * backup for the corresponding live configuration (returned by {@link Pair#a})
    * 
    * @param staticConnectors a list of pair of TransportConfiguration corresponding to the live - backup nodes
    */
   void setStaticConnectors(List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors);

   /**
    * Returns the period used to check if a client has failed to receive pings from the server.
    *   
    * Period is in milliseconds, default value is {@link HornetQClient#DEFAULT_CLIENT_FAILURE_CHECK_PERIOD}.
    * 
    * @return the period used to check if a client has failed to receive pings from the server
    */
   long getClientFailureCheckPeriod();

   /**
    * Sets the period (in milliseconds) used to check if a client has failed to receive pings from the server.
    * 
    * Value must be -1 (to disable) or greater than 0.
    * 
    * @param clientFailureCheckPeriod the period to check failure
    */
   void setClientFailureCheckPeriod(long clientFailureCheckPeriod);

   /**
    * When <code>true</code>, consumers created through this factory will create temporary files to cache large messages.
    * 
    * There is 1 temporary file created for each large message.
    * 
    * Default value is {@link HornetQClient#DEFAULT_CACHE_LARGE_MESSAGE_CLIENT}.
    * 
    * @return <code>true</code> if consumers created through this factory will cache large messages in temporary files, <code>false</code> else
    */
   boolean isCacheLargeMessagesClient();

   /**
    * Sets whether large messages received by consumers created through this factory will be cached in temporary files or not.
    * 
    * @param cached <code>true</code> to cache large messages in temporary files, <code>false</code> else
    */
   void setCacheLargeMessagesClient(boolean cached);

   /**
    * Returns the connection <em>time-to-live</em>.
    * This TTL determines how long the server will keep a connection alive in the absence of any data arriving from the client.
    * 
    * Value is in milliseconds, default value is {@link HornetQClient#DEFAULT_CONNECTION_TTL}.
    * 
    * @return the connection time-to-live in milliseconds
    */
   long getConnectionTTL();

   /**
    * Sets this factory's connections <em>time-to-live</em>.
    * 
    * Value must be -1 (to disable) or greater or equals to 0.
    * 
    * @param connectionTTL period in milliseconds
    */
   void setConnectionTTL(long connectionTTL);

   /**
    * Returns the blocking calls timeout.
    * 
    * If client's blocking calls to the server take more than this timeout, the call will throw a {@link HornetQException} with the code {@link HornetQException#CONNECTION_TIMEDOUT}.
    * Value is in milliseconds, default value is {@link HornetQClient#DEFAULT_CALL_TIMEOUT}.
    * 
    * @return the blocking calls timeout
    */
   long getCallTimeout();

   /**
    * Sets the blocking call timeout.
    * 
    * Value must be greater or equals to 0
    * 
    * @param callTimeout blocking call timeout in milliseconds
    */
   void setCallTimeout(long callTimeout);

   /**
    * Returns the large message size threshold.
    * 
    * Messages whose size is if greater than this value will be handled as <em>large messages</em>.
    * 
    * Value is in bytes, default value is {@link HornetQClient#DEFAULT_MIN_LARGE_MESSAGE_SIZE}.
    * 
    * @return the message size threshold to treat messages as large messages.
    */
   int getMinLargeMessageSize();

   /**
    * Sets the large message size threshold.
    * 
    * Value must be greater than 0.
    * 
    * @param minLargeMessageSize large message size threshold in bytes
    */
   void setMinLargeMessageSize(int minLargeMessageSize);

   /**
    * Returns the window size for flow control of the consumers created through this factory.
    * 
    * Value is in bytes, default value is {@link HornetQClient#DEFAULT_CONSUMER_WINDOW_SIZE}.
    * 
    * @return the window size used for consumer flow control
    */
   int getConsumerWindowSize();

   /**
    * Sets the window size for flow control of the consumers created through this factory.
    * 
    * Value must be -1 (to disable flow control), 0 (to not buffer any messages) or greater than 0 (to set the maximum size of the buffer)
    *
    * @param consumerWindowSize window size (in bytes) used for consumer flow control
    */
   void setConsumerWindowSize(int consumerWindowSize);

   /**
    * Returns the maximum rate of message consumption for consumers created through this factory.
    * 
    * This value controls the rate at which a consumer can consume messages. A consumer will never consume messages at a rate faster than the rate specified.
    * 
    * Value is -1 (to disable) or a positive integer corresponding to the maximum desired message consumption rate specified in units of messages per second.
    * Default value is {@link HornetQClient#DEFAULT_CONSUMER_MAX_RATE}.
    * 
    * @return the consumer max rate
    */
   int getConsumerMaxRate();

   /**
    * Sets the maximum rate of message consumption for consumers created through this factory.
    * 
    * Value must -1 (to disable) or a positive integer corresponding to the maximum desired message consumption rate specified in units of messages per second.
    * 
    * @param consumerMaxRate maximum rate of message consumption (in messages per seconds)
    */
   void setConsumerMaxRate(int consumerMaxRate);

   /**
    * Returns the size for the confirmation window of clients using this factory.
    * 
    * Value is in bytes or -1 (to disable the window). Default value is {@link HornetQClient#DEFAULT_CONFIRMATION_WINDOW_SIZE}.
    * 
    * @return the size for the confirmation window of clients using this factory
    */
   int getConfirmationWindowSize();

   /**
    * Sets the size for the confirmation window buffer of clients using this factory.
    * 
    * Value must be -1 (to disable the window) or greater than 0.

    * @param confirmationWindowSize size of the confirmation window (in bytes)
    */
   void setConfirmationWindowSize(int confirmationWindowSize);

   /**
    * Returns the window size for flow control of the producers created through this factory.
    * 
    * Value must be -1 (to disable flow control) or greater than 0 to determine the maximum amount of bytes at any give time (to prevent overloading the connection).
    * Default value is {@link HornetQClient#DEFAULT_PRODUCER_WINDOW_SIZE}.
    * 
    * @return the window size for flow control of the producers created through this factory.
    */
   int getProducerWindowSize();

   /**
    * Returns the window size for flow control of the producers created through this factory.
    * 
    * Value must be -1 (to disable flow control) or greater than 0.
    * 
    * @param producerWindowSize window size (in bytest) for flow control of the producers created through this factory.
    */
   void setProducerWindowSize(int producerWindowSize);

   /**
    * Returns the maximum rate of message production for producers created through this factory.
    * 
    * This value controls the rate at which a producer can produce messages. A producer will never produce messages at a rate faster than the rate specified.
    * 
    * Value is -1 (to disable) or a positive integer corresponding to the maximum desired message production rate specified in units of messages per second.
    * Default value is {@link HornetQClient#DEFAULT_PRODUCER_MAX_RATE}.
    * 
    * @return  maximum rate of message production (in messages per seconds)
    */
   int getProducerMaxRate();

   /**
    * Sets the maximum rate of message production for producers created through this factory.
    * 
    * Value must -1 (to disable) or a positive integer corresponding to the maximum desired message production rate specified in units of messages per second.
    * 
    * @param producerMaxRate maximum rate of message production (in messages per seconds)
    */
   void setProducerMaxRate(int producerMaxRate);

   /**
    * Returns whether consumers created through this factory will block while sending message acknowledgements or do it asynchronously.
    * 
    * Default value is {@link HornetQClient#DEFAULT_BLOCK_ON_ACKNOWLEDGE}.
    * 
    * @return whether consumers will block while sending message acknowledgements or do it asynchronously
    */
   boolean isBlockOnAcknowledge();

   /**
    * Sets whether consumers created through this factory will block while sending message acknowledgements or do it asynchronously.
    *
    * @param blockOnAcknowledge <code>true</code> to block when sending message acknowledgements or <code>false</code> to send them asynchronously
    */
   void setBlockOnAcknowledge(boolean blockOnAcknowledge);

   /**
    * Returns whether producers created through this factory will block while sending <em>durable</em> messages or do it asynchronously.
    * <br>
    * If the session is configured to send durable message asynchronously, the client can set a SendAcknowledgementHandler on the ClientSession
    * to be notified once the message has been handled by the server.
    * 
    * Default value is {@link HornetQClient#DEFAULT_BLOCK_ON_DURABLE_SEND}.
    *
    * @return whether producers will block while sending persistent messages or do it asynchronously
    */
   boolean isBlockOnDurableSend();

   /**
    * Sets whether producers created through this factory will block while sending <em>durable</em> messages or do it asynchronously.
    * 
    * @param blockOnDurableSend <code>true</code> to block when sending durable messages or <code>false</code> to send them asynchronously
    */
   void setBlockOnDurableSend(boolean blockOnDurableSend);

   /**
    * Returns whether producers created through this factory will block while sending <em>non-durable</em> messages or do it asynchronously.
    * <br>
    * If the session is configured to send non-durable message asynchronously, the client can set a SendAcknowledgementHandler on the ClientSession
    * to be notified once the message has been handled by the server.
    * 
    * Default value is {@link HornetQClient#DEFAULT_BLOCK_ON_NON_DURABLE_SEND}.
    *
    * @return whether producers will block while sending non-durable messages or do it asynchronously
    */
   boolean isBlockOnNonDurableSend();

   /**
    * Sets whether producers created through this factory will block while sending <em>non-durable</em> messages or do it asynchronously.
    * 
    * @param blockOnNonDurableSend <code>true</code> to block when sending non-durable messages or <code>false</code> to send them asynchronously
    */
   void setBlockOnNonDurableSend(boolean blockOnNonDurableSend);

   /**
    * Returns whether producers created through this factory will automatically
    * assign a group ID to the messages they sent.
    * 
    * if <code>true</code>, a random unique group ID is created and set on each message for the property
    * {@link org.hornetq.api.core.Message#HDR_GROUP_ID}.
    * Default value is {@link HornetQClient#DEFAULT_AUTO_GROUP}.
    * 
    * @return whether producers will automatically assign a group ID to their messages
    */
   boolean isAutoGroup();

   /**
    * Sets whether producers created through this factory will automatically
    * assign a group ID to the messages they sent.
    * 
    * @param autoGroup <code>true</code> to automatically assign a group ID to each messages sent through this factory, <code>false</code> else
    */
   void setAutoGroup(boolean autoGroup);

   /**
    * Returns the group ID that will be eventually set on each message for the property {@link org.hornetq.api.core.Message#HDR_GROUP_ID}.
    * 
    * Default value is is <code>null</code> and no group ID will be set on the messages.
    * 
    * @return the group ID that will be eventually set on each message
    */
   String getGroupID();
   
   /**
    * Sets the group ID that will be  set on each message sent through this factory.
    * 
    * @param groupID the group ID to use
    */
   void setGroupID(String groupID);

   /**
    * Returns whether messages will pre-acknowledged on the server before they are sent to the consumers or not.
    *
    * Default value is {@link HornetQClient#DEFAULT_PRE_ACKNOWLEDGE}
    */
   boolean isPreAcknowledge();

   /**
    * Sets to <code>true</code> to pre-acknowledge consumed messages on the server before they are sent to consumers, else set to <code>false</code> to let
    * clients acknowledge the message they consume.
    * 
    * @param preAcknowledge <code>true</code> to enable pre-acknowledgement, <code>false</code> else
    */
   void setPreAcknowledge(boolean preAcknowledge);

   /**
    * Returns the acknowledgements batch size.
    * 
    * Default value is  {@link HornetQClient#DEFAULT_ACK_BATCH_SIZE}.
    * 
    * @return the acknowledgements batch size
    */
   int getAckBatchSize();

   /**
    * Sets the acknowledgements batch size.
    * 
    * Value must be equal or greater than 0.
    * 
    * @param ackBatchSize acknowledgements batch size
    */
   void setAckBatchSize(int ackBatchSize);

   /**
    * Returns the local bind address to which the multicast socket is bound for discovery.
    * 
    * This is null if the multicast socket is not bound, or no discovery is being used
    * 
    * @return the local bind address to which the multicast socket is bound for discovery
    */
   String getLocalBindAddress();

   /**
    * Sets the local bind address to which the multicast socket is bound for discovery.
    * 
    * @param localBindAddress the local bind address
    */
   void setLocalBindAddress(String localBindAddress);
   
   /**
    * Returns the address to listen to discover which connectors this factory can use.
    * The discovery address must be set to enable this factory to discover HornetQ servers.
    * 
    * @return the address to listen to discover which connectors this factory can use
    */
   String getDiscoveryAddress();

   /**
    * Sets the address to listen to discover which connectors this factory can use.
    * 
    * @param discoveryAddress address to listen to discover which connectors this factory can use
    */
   void setDiscoveryAddress(String discoveryAddress);

   /**
    * Returns the port to listen to discover which connectors this factory can use.
    * The discovery port must be set to enable this factory to discover HornetQ servers.
    * 
    * @return the port to listen to discover which connectors this factory can use
    */   
   int getDiscoveryPort();


   /**
    * Sets the port to listen to discover which connectors this factory can use.
    * 
    * @param discoveryPort port to listen to discover which connectors this factory can use
    */
   void setDiscoveryPort(int discoveryPort);

   /**
    * Returns the refresh timeout for discovered HornetQ servers.
    * 
    * If this factory uses discovery to find HornetQ servers, the list of discovered servers
    * will be refreshed according to this timeout.
    * 
    * Value is in milliseconds, default value is {@link HornetQClient#DEFAULT_DISCOVERY_REFRESH_TIMEOUT}.
    * 
    * @return the refresh timeout for discovered HornetQ servers
    */
   long getDiscoveryRefreshTimeout();

   /**
    * Sets the refresh timeout for discovered HornetQ servers.
    * 
    * Value must be greater than 0.
    * 
    * @param discoveryRefreshTimeout refresh timeout (in milliseconds) for discovered HornetQ servers
    */
   void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout);

   /**
    * Returns the initial wait timeout if this factory is configured to use discovery.
    * 
    * Value is in milliseconds, default value is  {@link HornetQClient#DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT}.
    * 
    * @return the initial wait timeout if this factory is configured to use discovery 
    */
   long getDiscoveryInitialWaitTimeout();

   /**
    * Sets the initial wait timeout if this factory is configured to use discovery.
    * 
    * Value is in milliseconds and must be greater than 0.
    * 
    * @param initialWaitTimeout initial wait timeout when using discovery 
    */
   void setDiscoveryInitialWaitTimeout(long initialWaitTimeout);

   /**
    * Returns whether this factory will use global thread pools (shared among all the factories in the same JVM)
    * or its own pools.
    * 
    * Default value is {@link HornetQClient#DEFAULT_USE_GLOBAL_POOLS}.
    * 
    * @return <code>true</code> if this factory uses global thread pools, <code>false</code> else
    */
   boolean isUseGlobalPools();

   /**
    * Sets whether this factory will use global thread pools (shared among all the factories in the same JVM)
    * or its own pools.
    * 
    * @param useGlobalPools <code>true</code> to let this factory uses global thread pools, <code>false</code> else
    */
   void setUseGlobalPools(boolean useGlobalPools);

   /**
    * Returns the maximum size of the scheduled thread pool.
    * 
    * Default value is {@link HornetQClient#DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE}.
    * 
    * @return the maximum size of the scheduled thread pool.
    */
   int getScheduledThreadPoolMaxSize();

   /**
    * Sets the maximum size of the scheduled thread pool.
    * 
    * This setting is relevant only if this factory does not use global pools.
    * Value must be greater than 0.
    * 
    * @param scheduledThreadPoolMaxSize  maximum size of the scheduled thread pool.
    */
   void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize);

   /**
    * Returns the maximum size of the thread pool.
    * 
    * Default value is {@link HornetQClient#DEFAULT_THREAD_POOL_MAX_SIZE}.
    * 
    * @return the maximum size of the thread pool.
    */
   int getThreadPoolMaxSize();

   /**
    * Sets the maximum size of the thread pool.
    * 
    * This setting is relevant only if this factory does not use global pools.
    * Value must be -1 (for unlimited thread pool) or greater than 0.
    * 
    * @param threadPoolMaxSize  maximum size of the thread pool.
    */
   void setThreadPoolMaxSize(int threadPoolMaxSize);

   /**
    * Returns the time to retry connections created by this factory after failure. 
    * 
    * Value is in milliseconds, default is {@link HornetQClient#DEFAULT_RETRY_INTERVAL}.
    * 
    * @return the time to retry connections created by this factory after failure
    */
   long getRetryInterval();

   /**
    * Sets the time to retry connections created by this factory after failure.
    * 
    * Value must be greater than 0.
    * 
    * @param retryInterval time (in milliseconds) to retry connections created by this factory after failure 
    */
   void setRetryInterval(long retryInterval);

   /**
    * Returns the multiplier to apply to successive retry intervals.
    * 
    * Default value is  {@link HornetQClient#DEFAULT_RETRY_INTERVAL_MULTIPLIER}.
    * 
    * @return the multiplier to apply to successive retry intervals
    */
   double getRetryIntervalMultiplier();

   /**
    * Sets the multiplier to apply to successive retry intervals.
    * 
    * Value must be positive.
    * 
    * @param retryIntervalMultiplier multiplier to apply to successive retry intervals
    */
   void setRetryIntervalMultiplier(double retryIntervalMultiplier);

   /**
    * Returns the maximum retry interval (in the case a retry interval multiplier has been specified).
    * 
    * Value is in milliseconds, default value is  {@link HornetQClient#DEFAULT_MAX_RETRY_INTERVAL}.
    * 
    * @return the maximum retry interval
    */
   long getMaxRetryInterval();

   /**
    * Sets the maximum retry interval.
    * 
    * Value must be greater than 0.
    * 
    * @param maxRetryInterval maximum retry interval to apply in the case a retry interval multiplier has been specified
    */
   void setMaxRetryInterval(long maxRetryInterval);

   /**
    * Returns the maximum number of attempts to retry connection in case of failure.
    * 
    * Default value is {@link HornetQClient#DEFAULT_RECONNECT_ATTEMPTS}.
    * 
    * @return the maximum number of attempts to retry connection in case of failure.
    */
   int getReconnectAttempts();

   /**
    * Sets the maximum number of attempts to retry connection in case of failure.
    * 
    * Value must be -1 (to retry infinitely), 0 (to never retry connection) or greater than 0.
    * 
    * @param reconnectAttempts maximum number of attempts to retry connection in case of failure
    */
   void setReconnectAttempts(int reconnectAttempts);
   
   /**
    * Returns true if the client will automatically attempt to connect to the backup server if the initial
    * connection to the live server fails
    * 
    * Default value is {@link HornetQClient#DEFAULT_FAILOVER_ON_INITIAL_CONNECTION}.
    */
   boolean isFailoverOnInitialConnection();
   
   /**
    * Sets the value for FailoverOnInitialReconnection
    * 
    * @param failover
    */
   void setFailoverOnInitialConnection(boolean failover);

   /**
    * Returns whether connections created by this factory must failover in case the server they are
    * connected to <em>has normally shut down</em>.
    * 
    * Default value is {@link HornetQClient#DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN}.
    * 
    * @return <code>true</code> if connections must failover if the server has normally shut down, else <code>false</code>
    */
   boolean isFailoverOnServerShutdown();

   /**
    * Sets whether connections created by this factory must failover in case the server they are
    * connected to <em>has normally shut down</em>
    * 
    * @param failoverOnServerShutdown <code>true</code> if connections must failover if the server has normally shut down, <code>false</code> else
    */
   void setFailoverOnServerShutdown(boolean failoverOnServerShutdown);

   /**
    * Returns the class name of the connection load balancing policy.
    * 
    * Default value is "org.hornetq.api.core.client.loadbalance.RoundRobinConnectionLoadBalancingPolicy".
    * 
    * @return the class name of the connection load balancing policy
    */
   String getConnectionLoadBalancingPolicyClassName();

   /**
    * Sets the class name of the connection load balancing policy.
    * 
    * Value must be the name of a class implementing {@link ConnectionLoadBalancingPolicy}.
    * 
    * @param loadBalancingPolicyClassName class name of the connection load balancing policy
    */
   void setConnectionLoadBalancingPolicyClassName(String loadBalancingPolicyClassName);

   /**
    * Returns the initial size of messages created through this factory.
    * 
    * Value is in bytes, default value is  {@link HornetQClient#DEFAULT_INITIAL_MESSAGE_PACKET_SIZE}.
    * 
    * @return the initial size of messages created through this factory
    */
   int getInitialMessagePacketSize();

   /**
    * Sets the initial size of messages created through this factory.
    * 
    * Value must be greater than 0.
    * 
    * @param size initial size of messages created through this factory.
    */
   void setInitialMessagePacketSize(int size);
   
   /**
    * Adds an interceptor which will be executed <em>after packets are received from the server</em>.
    * 
    * @param interceptor an Interceptor
    */
   void addInterceptor(Interceptor interceptor);

   /**
    * Removes an interceptor.
    * 
    * @param interceptor interceptor to remove
    * 
    * @return <code>true</code> if the interceptor is removed from this factory, <code>false</code> else
    */
   boolean removeInterceptor(Interceptor interceptor);

   /**
    * Closes this factory and release all its resources
    */
   void close();

   /**
    * Creates a copy of this factory.
    * 
    * @return a copy of this factory with the same parameters values
    */
   ClientSessionFactory copy();

}
