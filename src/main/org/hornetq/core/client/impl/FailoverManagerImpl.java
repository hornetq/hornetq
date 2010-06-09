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

package org.hornetq.core.client.impl;

import java.lang.ref.WeakReference;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.api.core.client.loadbalance.ConnectionLoadBalancingPolicy;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.RemotingConnectionImpl;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.version.Version;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.spi.core.remoting.Connector;
import org.hornetq.spi.core.remoting.ConnectorFactory;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.UUIDGenerator;
import org.hornetq.utils.VersionLoader;

/**
 * A ConnectionManagerImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 27 Nov 2008 18:46:06
 *
 */
public class FailoverManagerImpl implements FailoverManager, ConnectionLifeCycleListener
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(FailoverManagerImpl.class);

   // debug

   private static Map<TransportConfiguration, Set<CoreRemotingConnection>> debugConns;

   private static boolean debug = false;

   public static void enableDebug()
   {
      FailoverManagerImpl.debug = true;

      FailoverManagerImpl.debugConns = new ConcurrentHashMap<TransportConfiguration, Set<CoreRemotingConnection>>();
   }

   public static void disableDebug()
   {
      FailoverManagerImpl.debug = false;

      FailoverManagerImpl.debugConns.clear();
      FailoverManagerImpl.debugConns = null;
   }

   private void checkAddDebug(final CoreRemotingConnection conn)
   {
      Set<CoreRemotingConnection> conns;

      synchronized (FailoverManagerImpl.debugConns)
      {
         conns = FailoverManagerImpl.debugConns.get(connectorConfig);

         if (conns == null)
         {
            conns = new HashSet<CoreRemotingConnection>();

            FailoverManagerImpl.debugConns.put(connectorConfig, conns);
         }

         conns.add(conn);
      }
   }

   public static void failAllConnectionsForConnector(final TransportConfiguration config)
   {
      Set<CoreRemotingConnection> conns;

      synchronized (FailoverManagerImpl.debugConns)
      {
         conns = FailoverManagerImpl.debugConns.get(config);

         if (conns != null)
         {
            conns = new HashSet<CoreRemotingConnection>(FailoverManagerImpl.debugConns.get(config));
         }
      }

      if (conns != null)
      {
         for (CoreRemotingConnection conn : conns)
         {
            conn.fail(new HornetQException(HornetQException.INTERNAL_ERROR, "simulated connection failure"));
         }
      }
   }

   // Attributes
   // -----------------------------------------------------------------------------------

   private final TransportConfiguration connectorConfig;

   private ConnectorFactory connectorFactory;

   private Map<String, Object> transportParams;

   private ConnectorFactory backupConnectorFactory;

   private Map<String, Object> backupTransportParams;

   private final long callTimeout;

   private final long clientFailureCheckPeriod;

   private final long connectionTTL;

   private final Set<ClientSessionInternal> sessions = new HashSet<ClientSessionInternal>();

   private final Object exitLock = new Object();

   private final Object createSessionLock = new Object();

   private boolean inCreateSession;

   private final Object failoverLock = new Object();

   private final ExecutorFactory orderedExecutorFactory;

   private final ExecutorService threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private final Executor closeExecutor;

   private CoreRemotingConnection connection;

   private final long retryInterval;

   private final double retryIntervalMultiplier; // For exponential backoff

   private final long maxRetryInterval;

   private final int reconnectAttempts;

   private final boolean failoverOnServerShutdown;

   private final Set<SessionFailureListener> listeners = new ConcurrentHashSet<SessionFailureListener>();

   private Connector connector;

   private Future<?> pingerFuture;

   private PingRunnable pingRunnable;

   private volatile boolean exitLoop;

   private final List<Interceptor> interceptors;

   private volatile boolean stopPingingAfterOne;

   private final boolean failoverOnInitialConnection;

   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   public FailoverManagerImpl(final ClientSessionFactory sessionFactory,
                              final TransportConfiguration connectorConfig,
                              final TransportConfiguration backupConfig,
                              final boolean failoverOnServerShutdown,
                              final long callTimeout,
                              final long clientFailureCheckPeriod,
                              final long connectionTTL,
                              final long retryInterval,
                              final double retryIntervalMultiplier,
                              final long maxRetryInterval,
                              final int reconnectAttempts,
                              final boolean failoverOnInitialConnection,
                              final ExecutorService threadPool,
                              final ScheduledExecutorService scheduledThreadPool,
                              final List<Interceptor> interceptors)
   {
      this.connectorConfig = connectorConfig;

      this.failoverOnServerShutdown = failoverOnServerShutdown;

      connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());

      transportParams = connectorConfig.getParams();

      checkTransportKeys(connectorFactory, transportParams);

      if (backupConfig != null)
      {
         backupConnectorFactory = instantiateConnectorFactory(backupConfig.getFactoryClassName());

         backupTransportParams = backupConfig.getParams();

         checkTransportKeys(backupConnectorFactory, backupTransportParams);
      }
      else
      {
         backupConnectorFactory = null;

         backupTransportParams = null;
      }

      this.callTimeout = callTimeout;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.reconnectAttempts = reconnectAttempts;

      this.failoverOnInitialConnection = failoverOnInitialConnection;

      this.scheduledThreadPool = scheduledThreadPool;

      this.threadPool = threadPool;

      orderedExecutorFactory = new OrderedExecutorFactory(threadPool);

      closeExecutor = orderedExecutorFactory.getExecutor();

      this.interceptors = interceptors;
   }

   // ConnectionLifeCycleListener implementation --------------------------------------------------

   public void connectionCreated(final Connection connection, final ProtocolType protocol)
   {
   }

   public void connectionDestroyed(final Object connectionID)
   {
      handleConnectionFailure(connectionID,
                              new HornetQException(HornetQException.NOT_CONNECTED, "Channel disconnected"));
   }

   public void connectionException(final Object connectionID, final HornetQException me)
   {
      handleConnectionFailure(connectionID, me);
   }

   // ConnectionManager implementation ------------------------------------------------------------------

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final int ackBatchSize,
                                      final boolean cacheLargeMessageClient,
                                      final int minLargeMessageSize,
                                      final boolean blockOnAcknowledge,
                                      final boolean autoGroup,
                                      final int confWindowSize,
                                      final int producerWindowSize,
                                      final int consumerWindowSize,
                                      final int producerMaxRate,
                                      final int consumerMaxRate,
                                      final boolean blockOnNonDurableSend,
                                      final boolean blockOnDurableSend,
                                      final int initialMessagePacketSize,
                                      final String groupID) throws HornetQException
   {
      synchronized (createSessionLock)
      {
         String name = UUIDGenerator.getInstance().generateSimpleStringUUID().toString();

         boolean retry = false;
         do
         {
            Version clientVersion = VersionLoader.getVersion();

            CoreRemotingConnection theConnection = null;

            Lock lock = null;

            try
            {
               Channel channel1;

               synchronized (failoverLock)
               {
                  theConnection = getConnectionWithRetry(reconnectAttempts);

                  if (theConnection == null)
                  {
                     if (exitLoop)
                     {
                        return null;
                     }

                     if (failoverOnInitialConnection && backupConnectorFactory != null)
                     {
                        // Try and connect to the backup

                        log.warn("Server is not available to make initial connection to. Will " + "try backup server instead.");

                        connectorFactory = backupConnectorFactory;

                        transportParams = backupTransportParams;

                        backupConnectorFactory = null;

                        backupTransportParams = null;

                        theConnection = getConnectionWithRetry(reconnectAttempts);
                     }

                     if (exitLoop)
                     {
                        return null;
                     }

                     if (theConnection == null)
                     {
                        throw new HornetQException(HornetQException.NOT_CONNECTED,
                                                   "Unable to connect to server using configuration " + connectorConfig);
                     }
                  }

                  channel1 = theConnection.getChannel(1, -1);

                  // Lock it - this must be done while the failoverLock is held
                  channel1.getLock().lock();

                  lock = channel1.getLock();
               } // We can now release the failoverLock

               // We now set a flag saying createSession is executing
               synchronized (exitLock)
               {
                  inCreateSession = true;
               }

               long sessionChannelID = theConnection.generateChannelID();

               Packet request = new CreateSessionMessage(name,
                                                         sessionChannelID,
                                                         clientVersion.getIncrementingVersion(),
                                                         username,
                                                         password,
                                                         minLargeMessageSize,
                                                         xa,
                                                         autoCommitSends,
                                                         autoCommitAcks,
                                                         preAcknowledge,
                                                         confWindowSize,
                                                         null);

               Packet pResponse;
               try
               {
                  pResponse = channel1.sendBlocking(request);
               }
               catch (HornetQException e)
               {
                  if (e.getCode() == HornetQException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS)
                  {
                     theConnection.destroy();
                  }

                  if (e.getCode() == HornetQException.UNBLOCKED)
                  {
                     // This means the thread was blocked on create session and failover unblocked it
                     // so failover could occur

                     retry = true;

                     continue;
                  }
                  else
                  {
                     throw e;
                  }
               }

               CreateSessionResponseMessage response = (CreateSessionResponseMessage)pResponse;

               Channel sessionChannel = theConnection.getChannel(sessionChannelID, confWindowSize);

               ClientSessionInternal session = new ClientSessionImpl(this,
                                                                     name,
                                                                     username,
                                                                     password,
                                                                     xa,
                                                                     autoCommitSends,
                                                                     autoCommitAcks,
                                                                     preAcknowledge,
                                                                     blockOnAcknowledge,
                                                                     autoGroup,
                                                                     ackBatchSize,
                                                                     consumerWindowSize,
                                                                     consumerMaxRate,
                                                                     confWindowSize,
                                                                     producerWindowSize,
                                                                     producerMaxRate,
                                                                     blockOnNonDurableSend,
                                                                     blockOnDurableSend,
                                                                     cacheLargeMessageClient,
                                                                     minLargeMessageSize,
                                                                     initialMessagePacketSize,
                                                                     groupID,
                                                                     theConnection,
                                                                     response.getServerVersion(),
                                                                     sessionChannel,
                                                                     orderedExecutorFactory.getExecutor());

               sessions.add(session);

               ChannelHandler handler = new ClientSessionPacketHandler(session, sessionChannel);

               sessionChannel.setHandler(handler);

               return new DelegatingSession(session);
            }
            catch (Throwable t)
            {
               if (lock != null)
               {
                  lock.unlock();

                  lock = null;
               }

               if (t instanceof HornetQException)
               {
                  throw (HornetQException)t;
               }
               else
               {
                  HornetQException me = new HornetQException(HornetQException.INTERNAL_ERROR,
                                                             "Failed to create session",
                                                             t);

                  throw me;
               }
            }
            finally
            {
               if (lock != null)
               {
                  lock.unlock();
               }

               // Execution has finished so notify any failover thread that may be waiting for us to be done
               synchronized (exitLock)
               {
                  inCreateSession = false;

                  exitLock.notify();
               }
            }
         }
         while (retry);
      }

      // Should never get here
      throw new IllegalStateException("Oh my God it's full of stars!");
   }

   // Must be synchronized to prevent it happening concurrently with failover which can lead to
   // inconsistencies
   public void removeSession(final ClientSessionInternal session)
   {
      // TODO - can we simplify this locking?
      synchronized (createSessionLock)
      {
         synchronized (failoverLock)
         {
            sessions.remove(session);

            checkCloseConnection();
         }
      }
   }

   public synchronized int numConnections()
   {
      return connection != null ? 1 : 0;
   }

   public int numSessions()
   {
      return sessions.size();
   }

   public void addFailureListener(final SessionFailureListener listener)
   {
      listeners.add(listener);
   }

   public boolean removeFailureListener(final SessionFailureListener listener)
   {
      return listeners.remove(listener);
   }

   public void causeExit()
   {
      exitLoop = true;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   public void stopPingingAfterOne()
   {
      stopPingingAfterOne = true;
   }

   // Protected
   // ------------------------------------------------------------------------------------

   // Package Private
   // ------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void handleConnectionFailure(final Object connectionID, final HornetQException me)
   {
      failoverOrReconnect(connectionID, me);
   }

   private void failoverOrReconnect(final Object connectionID, final HornetQException me)
   {
      Set<ClientSessionInternal> sessionsToClose = null;

      synchronized (failoverLock)
      {
         if (connection == null || connection.getID() != connectionID)
         {
            // We already failed over/reconnected - probably the first failure came in, all the connections were failed
            // over then a async connection exception or disconnect
            // came in for one of the already exitLoop connections, so we return true - we don't want to call the
            // listeners again

            return;
         }

         // We call before reconnection occurs to give the user a chance to do cleanup, like cancel messages
         callFailureListeners(me, false);

         // Now get locks on all channel 1s, whilst holding the failoverLock - this makes sure
         // There are either no threads executing in createSession, or one is blocking on a createSession
         // result.

         // Then interrupt the channel 1 that is blocking (could just interrupt them all)

         // Then release all channel 1 locks - this allows the createSession to exit the monitor

         // Then get all channel 1 locks again - this ensures the any createSession thread has executed the section and
         // returned all its connections to the connection manager (the code to return connections to connection manager
         // must be inside the lock

         // Then perform failover

         // Then release failoverLock

         // The other side of the bargain - during createSession:
         // The calling thread must get the failoverLock and get its' connections when this is locked.
         // While this is still locked it must then get the channel1 lock
         // It can then release the failoverLock
         // It should catch HornetQException.INTERRUPTED in the call to channel.sendBlocking
         // It should then return its connections, with channel 1 lock still held
         // It can then release the channel 1 lock, and retry (which will cause locking on failoverLock
         // until failover is complete

         boolean serverShutdown = me.getCode() == HornetQException.DISCONNECTED;

         // We will try to failover if there is a backup connector factory, but we don't do this if the server
         // has been shutdown cleanly unless failoverOnServerShutdown is true
         boolean attemptFailover = backupConnectorFactory != null && (failoverOnServerShutdown || !serverShutdown);

         boolean attemptReconnect;

         if (attemptFailover)
         {
            attemptReconnect = false;
         }
         else
         {
            attemptReconnect = reconnectAttempts != 0;
         }

         if (attemptFailover || attemptReconnect)
         {
            lockChannel1();

            final boolean needToInterrupt;

            synchronized (exitLock)
            {
               needToInterrupt = inCreateSession;
            }

            unlockChannel1();

            if (needToInterrupt)
            {
               // Forcing return all channels won't guarantee that any blocked thread will return immediately
               // So we need to wait for it
               forceReturnChannel1();

               // Now we need to make sure that the thread has actually exited and returned it's connections
               // before failover occurs

               synchronized (exitLock)
               {
                  while (inCreateSession)
                  {
                     try
                     {
                        exitLock.wait(5000);
                     }
                     catch (InterruptedException e)
                     {
                     }
                  }
               }
            }

            // Now we absolutely know that no threads are executing in or blocked in createSession, and no
            // more will execute it until failover is complete

            // So.. do failover / reconnection

            CoreRemotingConnection oldConnection = connection;

            connection = null;

            try
            {
               connector.close();
            }
            catch (Exception ignore)
            {
            }

            cancelScheduledTasks();

            connector = null;

            if (attemptFailover)
            {
               // Now try failing over to backup

               connectorFactory = backupConnectorFactory;

               transportParams = backupTransportParams;

               backupConnectorFactory = null;

               backupTransportParams = null;

               reconnectSessions(oldConnection, reconnectAttempts == -1 ? -1 : reconnectAttempts + 1);
            }
            else
            {
               reconnectSessions(oldConnection, reconnectAttempts);
            }

            oldConnection.destroy();
         }
         else
         {
            connection.destroy();

            connection = null;
         }

         callFailureListeners(me, true);

         if (connection == null)
         {
            sessionsToClose = new HashSet<ClientSessionInternal>(sessions);
         }
      }

      // This needs to be outside the failover lock to prevent deadlock
      if (sessionsToClose != null)
      {
         // If connection is null it means we didn't succeed in failing over or reconnecting
         // so we close all the sessions, so they will throw exceptions when attempted to be used

         for (ClientSessionInternal session : sessionsToClose)
         {
            try
            {
               session.cleanUp();
            }
            catch (Exception e)
            {
               FailoverManagerImpl.log.error("Failed to cleanup session");
            }
         }
      }
   }

   private void callFailureListeners(final HornetQException me, final boolean afterReconnect)
   {
      final List<SessionFailureListener> listenersClone = new ArrayList<SessionFailureListener>(listeners);

      for (final SessionFailureListener listener : listenersClone)
      {
         try
         {
            if (afterReconnect)
            {
               listener.connectionFailed(me);
            }
            else
            {
               listener.beforeReconnect(me);
            }
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            FailoverManagerImpl.log.error("Failed to execute failure listener", t);
         }
      }
   }

   /*
    * Re-attach sessions all pre-existing sessions to the new remoting connection
    */
   private void reconnectSessions(final CoreRemotingConnection oldConnection, final int reconnectAttempts)
   {
      CoreRemotingConnection newConnection = getConnectionWithRetry(reconnectAttempts);

      if (newConnection == null)
      {
         FailoverManagerImpl.log.warn("Failed to connect to server.");

         return;
      }

      List<FailureListener> oldListeners = oldConnection.getFailureListeners();

      List<FailureListener> newListeners = new ArrayList<FailureListener>(newConnection.getFailureListeners());

      for (FailureListener listener : oldListeners)
      {
         // Add all apart from the first one which is the old DelegatingFailureListener

         if (listener instanceof DelegatingFailureListener == false)
         {
            newListeners.add(listener);
         }
      }

      newConnection.setFailureListeners(newListeners);

      for (ClientSessionInternal session : sessions)
      {
         session.handleFailover(newConnection);
      }
   }

   private CoreRemotingConnection getConnectionWithRetry(final int reconnectAttempts)
   {
      long interval = retryInterval;

      int count = 0;

      while (true)
      {
         if (exitLoop)
         {
            return null;
         }

         CoreRemotingConnection theConnection = getConnection();

         if (theConnection == null)
         {
            // Failed to get connection

            if (reconnectAttempts != 0)
            {
               count++;

               if (reconnectAttempts != -1 && count == reconnectAttempts)
               {
                  FailoverManagerImpl.log.warn("Tried " + reconnectAttempts + " times to connect. Now giving up.");

                  return null;
               }

               try
               {
                  Thread.sleep(interval);
               }
               catch (InterruptedException ignore)
               {
               }

               // Exponential back-off
               long newInterval = (long)(interval * retryIntervalMultiplier);

               if (newInterval > maxRetryInterval)
               {
                  newInterval = maxRetryInterval;
               }

               interval = newInterval;
            }
            else
            {
               return null;
            }
         }
         else
         {

            if (FailoverManagerImpl.debug)
            {
               checkAddDebug(theConnection);
            }

            return theConnection;
         }
      }
   }

   private void cancelScheduledTasks()
   {
      if (pingerFuture != null)
      {
         pingRunnable.cancel();

         pingerFuture.cancel(false);

         pingRunnable = null;

         pingerFuture = null;
      }
   }

   private void checkCloseConnection()
   {
      if (connection != null && sessions.size() == 0)
      {
         cancelScheduledTasks();

         try
         {
            connection.destroy();
         }
         catch (Throwable ignore)
         {
         }

         connection = null;

         try
         {
            if (connector != null)
            {
               connector.close();
            }
         }
         catch (Throwable ignore)
         {
         }

         connector = null;
      }
   }

   public CoreRemotingConnection getConnection()
   {
      if (connection == null)
      {
         Connection tc = null;

         try
         {
            DelegatingBufferHandler handler = new DelegatingBufferHandler();

            connector = connectorFactory.createConnector(transportParams,
                                                         handler,
                                                         this,
                                                         closeExecutor,
                                                         threadPool,
                                                         scheduledThreadPool);

            if (connector != null)
            {
               connector.start();

               tc = connector.createConnection();

               if (tc == null)
               {
                  try
                  {
                     connector.close();
                  }
                  catch (Throwable t)
                  {
                  }

                  connector = null;
               }
            }
         }
         catch (Exception e)
         {
            // Sanity catch for badly behaved remoting plugins

            FailoverManagerImpl.log.warn("connector.create or connectorFactory.createConnector should never throw an exception, implementation is badly behaved, but we'll deal with it anyway.",
                                         e);

            if (tc != null)
            {
               try
               {
                  tc.close();
               }
               catch (Throwable t)
               {
               }
            }

            if (connector != null)
            {
               try
               {
                  connector.close();
               }
               catch (Throwable t)
               {
               }
            }

            tc = null;

            connector = null;
         }

         if (tc == null)
         {
            return connection;
         }

         connection = new RemotingConnectionImpl(tc, callTimeout, interceptors);

         connection.addFailureListener(new DelegatingFailureListener(connection.getID()));

         connection.getChannel(0, -1).setHandler(new Channel0Handler(connection));

         if (clientFailureCheckPeriod != -1)
         {
            if (pingerFuture == null)
            {
               pingRunnable = new PingRunnable();

               pingerFuture = scheduledThreadPool.scheduleWithFixedDelay(new ActualScheduledPinger(pingRunnable),
                                                                         0,
                                                                         clientFailureCheckPeriod,
                                                                         TimeUnit.MILLISECONDS);
            }
            // send a ping every time we create a new remoting connection
            // to set up its TTL on the server side
            else
            {
               pingRunnable.run();
            }
         }
      }

      return connection;
   }

   private ConnectorFactory instantiateConnectorFactory(final String connectorFactoryClassName)
   {
      return AccessController.doPrivileged(new PrivilegedAction<ConnectorFactory>()
      {
         public ConnectorFactory run()
         {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try
            {
               Class<?> clazz = loader.loadClass(connectorFactoryClassName);
               return (ConnectorFactory)clazz.newInstance();
            }
            catch (Exception e)
            {
               throw new IllegalArgumentException("Error instantiating connector factory \"" + connectorFactoryClassName +
                                                  "\"", e);
            }
         }
      });

   }

   private void lockChannel1()
   {
      Channel channel1 = connection.getChannel(1, -1);

      channel1.getLock().lock();
   }

   private void unlockChannel1()
   {
      Channel channel1 = connection.getChannel(1, -1);

      channel1.getLock().unlock();
   }

   private void forceReturnChannel1()
   {
      Channel channel1 = connection.getChannel(1, -1);

      channel1.returnBlocking();
   }

   private void checkTransportKeys(final ConnectorFactory factory, final Map<String, Object> params)
   {
      if (params != null)
      {
         Set<String> invalid = ConfigurationHelper.checkKeys(factory.getAllowableProperties(), params.keySet());

         if (!invalid.isEmpty())
         {
            String msg = ConfigurationHelper.stringSetToCommaListString("The following keys are invalid for configuring a connector: ",
                                                                        invalid);

            throw new IllegalStateException(msg);

         }
      }
   }

   private class Channel0Handler implements ChannelHandler
   {
      private final CoreRemotingConnection conn;

      private Channel0Handler(final CoreRemotingConnection conn)
      {
         this.conn = conn;
      }

      public void handlePacket(final Packet packet)
      {
         final byte type = packet.getType();

         if (type == PacketImpl.DISCONNECT)
         {
            closeExecutor.execute(new Runnable()
            {
               // Must be executed on new thread since cannot block the netty thread for a long time and fail can
               // cause reconnect loop
               public void run()
               {
                  conn.fail(new HornetQException(HornetQException.DISCONNECTED,
                                                 "The connection was disconnected because of server shutdown"));
               }
            });
         }
      }
   }

   private class DelegatingBufferHandler implements BufferHandler
   {
      public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
      {
         CoreRemotingConnection theConn = connection;

         if (theConn != null && connectionID == theConn.getID())
         {
            theConn.bufferReceived(connectionID, buffer);
         }
      }
   }

   private class DelegatingFailureListener implements FailureListener
   {
      private final Object connectionID;

      DelegatingFailureListener(final Object connectionID)
      {
         this.connectionID = connectionID;
      }

      public void connectionFailed(final HornetQException me)
      {
         handleConnectionFailure(connectionID, me);
      }
   }

   private static final class ActualScheduledPinger implements Runnable
   {
      private final WeakReference<PingRunnable> pingRunnable;

      ActualScheduledPinger(final PingRunnable runnable)
      {
         pingRunnable = new WeakReference<PingRunnable>(runnable);
      }

      public void run()
      {
         PingRunnable runnable = pingRunnable.get();

         if (runnable != null)
         {
            runnable.run();
         }
      }

   }

   private final class PingRunnable implements Runnable
   {
      private boolean cancelled;

      private boolean first;

      private long lastCheck = System.currentTimeMillis();

      public synchronized void run()
      {
         if (cancelled || stopPingingAfterOne && !first)
         {
            return;
         }

         first = false;

         long now = System.currentTimeMillis();

         if (clientFailureCheckPeriod != -1 && now >= lastCheck + clientFailureCheckPeriod)
         {
            if (!connection.checkDataReceived())
            {
               final HornetQException me = new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                                                "Did not receive data from server for " + connection.getTransportConnection());

               cancelled = true;

               threadPool.execute(new Runnable()
               {
                  // Must be executed on different thread
                  public void run()
                  {
                     connection.fail(me);
                  }
               });

               return;
            }
            else
            {
               lastCheck = now;
            }
         }

         // Send a ping

         Ping ping = new Ping(connectionTTL);

         Channel channel0 = connection.getChannel(0, -1);

         channel0.send(ping);

         connection.flush();
      }

      public synchronized void cancel()
      {
         cancelled = true;
      }
   }

}
