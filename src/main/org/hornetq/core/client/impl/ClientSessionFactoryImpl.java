/*
 * Copyright 2010 Red Hat, Inc.
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
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.RemotingConnectionImpl;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V2;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.DisconnectMessage;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.version.Version;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.Acceptor;
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
 * A ClientSessionFactoryImpl
 *
 * Encapsulates a connection to a server
 *
 * @author Tim Fox
 *
 *
 */
public class ClientSessionFactoryImpl implements ClientSessionFactoryInternal, ConnectionLifeCycleListener
{

   // Constants
   // ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(ClientSessionFactoryImpl.class);

   private static final boolean isTrace = ClientSessionFactoryImpl.log.isTraceEnabled();

   private static final boolean isDebug = ClientSessionFactoryImpl.log.isDebugEnabled();

   // Attributes
   // -----------------------------------------------------------------------------------

   private final ServerLocatorInternal serverLocator;

   private TransportConfiguration connectorConfig;

   private TransportConfiguration backupConfig;

   private ConnectorFactory connectorFactory;

   private transient boolean finalizeCheck = true;

   private final long callTimeout;

   private final long clientFailureCheckPeriod;

   private final long connectionTTL;

   private final Set<ClientSessionInternal> sessions = new HashSet<ClientSessionInternal>();

   private final Object exitLock = new Object();

   private final Object createSessionLock = new Object();

   private boolean inCreateSession;

   private final Object failoverLock = new Object();

   private final ExecutorFactory orderedExecutorFactory;

   private final Executor threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private final Executor closeExecutor;

   private CoreRemotingConnection connection;

   private final long retryInterval;

   private final double retryIntervalMultiplier; // For exponential backoff

   private final long maxRetryInterval;

   private int reconnectAttempts;

   private final Set<SessionFailureListener> listeners = new ConcurrentHashSet<SessionFailureListener>();

   private Connector connector;

   private Future<?> pingerFuture;

   private PingRunnable pingRunnable;

   private volatile boolean exitLoop;

   private final List<Interceptor> interceptors;

   private volatile boolean stopPingingAfterOne;

   private volatile boolean closed;

   public final Exception e = new Exception();

   private final Object waitLock = new Object();

   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   public ClientSessionFactoryImpl(final ServerLocatorInternal serverLocator,
                                   final TransportConfiguration connectorConfig,
                                   final long callTimeout,
                                   final long clientFailureCheckPeriod,
                                   final long connectionTTL,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final long maxRetryInterval,
                                   final int reconnectAttempts,
                                   final Executor threadPool,
                                   final ScheduledExecutorService scheduledThreadPool,
                                   final List<Interceptor> interceptors)
   {

      e.fillInStackTrace();

      this.serverLocator = serverLocator;

      this.connectorConfig = connectorConfig;

      connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());

      checkTransportKeys(connectorFactory, connectorConfig.getParams());

      this.callTimeout = callTimeout;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.reconnectAttempts = reconnectAttempts;

      this.scheduledThreadPool = scheduledThreadPool;

      this.threadPool = threadPool;

      orderedExecutorFactory = new OrderedExecutorFactory(threadPool);

      closeExecutor = orderedExecutorFactory.getExecutor();

      this.interceptors = interceptors;

   }

   public void disableFinalizeCheck()
   {
      finalizeCheck = false;
   }

   public void connect(final int initialConnectAttempts, final boolean failoverOnInitialConnection) throws HornetQException
   {
      // Get the connection
      getConnectionWithRetry(initialConnectAttempts);

      if (connection == null)
      {
         StringBuffer msg = new StringBuffer("Unable to connect to server using configuration ").append(connectorConfig);
         if (backupConfig != null)
         {
            msg.append(" and backup configuration ").append(backupConfig);
         }
         throw new HornetQException(HornetQException.NOT_CONNECTED, msg.toString());
      }

   }

   public TransportConfiguration getConnectorConfiguration()
   {
      return connectorConfig;
   }

   public void setBackupConnector(final TransportConfiguration live, final TransportConfiguration backUp)
   {
      if (live.equals(connectorConfig) && backUp != null && !backUp.equals(connectorConfig))
      {
         if (ClientSessionFactoryImpl.isDebug)
         {
            ClientSessionFactoryImpl.log.debug("Setting up backup config = " + backUp + " for live = " + live);
         }
         backupConfig = backUp;
      }
      else
      {
         if (ClientSessionFactoryImpl.isDebug)
         {
            ClientSessionFactoryImpl.log.debug("ClientSessionFactoryImpl received backup update for live/backup pair = " + live +
                                               " / " +
                                               backUp +
                                               " but it didn't belong to " +
                                               connectorConfig);
         }
      }
   }

   public Object getBackupConnector()
   {
      return backupConfig;
   }

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final int ackBatchSize) throws HornetQException
   {
      return createSessionInternal(username,
                                   password,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   ackBatchSize);
   }

   public ClientSession createSession(final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final int ackBatchSize) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   serverLocator.isPreAcknowledge(),
                                   ackBatchSize);
   }

   public ClientSession createXASession() throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   true,
                                   false,
                                   false,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createTransactedSession() throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   false,
                                   false,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createSession() throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   true,
                                   true,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createSession(final boolean autoCommitSends, final boolean autoCommitAcks) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createSession(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   serverLocator.isPreAcknowledge(),
                                   serverLocator.getAckBatchSize());
   }

   public ClientSession createSession(final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   serverLocator.getAckBatchSize());
   }

   // ConnectionLifeCycleListener implementation --------------------------------------------------

   public void connectionCreated(final Acceptor acceptor, final Connection connection, final ProtocolType protocol)
   {
   }

   public void connectionDestroyed(final Object connectionID)
   {
      // It has to use the same executor as the disconnect message is being sent through

      final HornetQException ex = new HornetQException(HornetQException.NOT_CONNECTED, "Channel disconnected");

      closeExecutor.execute(new Runnable()
      {
         public void run()
         {
            handleConnectionFailure(connectionID, ex);
         }
      });

   }

   public void connectionException(final Object connectionID, final HornetQException me)
   {
      handleConnectionFailure(connectionID, me);
   }

   // Must be synchronized to prevent it happening concurrently with failover which can lead to
   // inconsistencies
   public void removeSession(final ClientSessionInternal session, final boolean failingOver)
   {
      synchronized (sessions)
      {
         sessions.remove(session);
      }
   }

   public void connectionReadyForWrites(final Object connectionID, final boolean ready)
   {
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
      synchronized (waitLock)
      {
         exitLoop = true;
         waitLock.notifyAll();
      }
   }

   public void close()
   {
      if (closed)
      {
         return;
      }

      synchronized (exitLock)
      {
         exitLock.notifyAll();
      }

      forceReturnChannel1();

      // we need to stop the factory from connecting if it is in the middle of trying to failover before we get the lock
      causeExit();
      synchronized (createSessionLock)
      {
         synchronized (failoverLock)
         {
            HashSet<ClientSession> sessionsToClose;
            synchronized (sessions)
            {
               sessionsToClose = new HashSet<ClientSession>(sessions);
            }
            // work on a copied set. the session will be removed from sessions when session.close() is called
            for (ClientSession session : sessionsToClose)
            {
               try
               {
                  session.close();
               }
               catch (HornetQException e)
               {
                  ClientSessionFactoryImpl.log.warn("Unable to close session", e);
               }
            }

            checkCloseConnection();
         }
      }

      closed = true;

      serverLocator.factoryClosed(this);
   }

   public void cleanup()
   {
      if (closed)
      {
         return;
      }

      // we need to stop the factory from connecting if it is in the middle of trying to failover before we get the lock
      causeExit();
      synchronized (createSessionLock)
      {
         HashSet<ClientSessionInternal> sessionsToClose;
         synchronized (sessions)
         {
            sessionsToClose = new HashSet<ClientSessionInternal>(sessions);
         }
         // work on a copied set. the session will be removed from sessions when session.close() is called
         for (ClientSessionInternal session : sessionsToClose)
         {
            try
            {
               session.cleanUp(false);
            }
            catch (Exception e)
            {
               ClientSessionFactoryImpl.log.warn("Unable to close session", e);
            }
         }

         checkCloseConnection();
      }

      closed = true;
   }

   public boolean isClosed()
   {
      return closed || serverLocator.isClosed();
   }

   public ServerLocator getServerLocator()
   {
      return serverLocator;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   public void stopPingingAfterOne()
   {
      stopPingingAfterOne = true;
   }

   public void resumePinging()
   {
      stopPingingAfterOne = false;
   }

   // Protected
   // ------------------------------------------------------------------------------------

   // Package Private
   // ------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void handleConnectionFailure(final Object connectionID, final HornetQException me)
   {
      try
      {
         failoverOrReconnect(connectionID, me);
      }
      catch (HornetQInterruptedException e)
      {
         // this is just a debug, since an interrupt is an expected event (in case of a shutdown)
         log.debug(e.getMessage(), e);
      }
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

         if (ClientSessionFactoryImpl.isTrace)
         {
            ClientSessionFactoryImpl.log.trace("Client Connection failed, calling failure listeners and trying to reconnect, reconnectAttempts=" + reconnectAttempts);
         }

         // We call before reconnection occurs to give the user a chance to do cleanup, like cancel messages
         callFailureListeners(me, false, false);

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

         if (reconnectAttempts != 0)
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
                        throw new HornetQInterruptedException(e);
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

            reconnectSessions(oldConnection, reconnectAttempts);

            if (oldConnection != null)
            {
               oldConnection.destroy();
            }
         }
         else
         {
            CoreRemotingConnection connectionToDestory = connection;
            if (connectionToDestory != null)
            {
               connectionToDestory.destroy();
            }

            connection = null;
         }

         if (connection == null)
         {
            synchronized (sessions)
            {
               sessionsToClose = new HashSet<ClientSessionInternal>(sessions);
            }
            callFailureListeners(me, true, false);
         }
      }

      // This needs to be outside the failover lock to prevent deadlock
      if (connection != null)
      {
         callFailureListeners(me, true, true);
      }
      if (sessionsToClose != null)
      {
         // If connection is null it means we didn't succeed in failing over or reconnecting
         // so we close all the sessions, so they will throw exceptions when attempted to be used

         for (ClientSessionInternal session : sessionsToClose)
         {
            try
            {
               session.cleanUp(true);
            }
            catch (Exception e)
            {
               ClientSessionFactoryImpl.log.error("Failed to cleanup session");
            }
         }
      }
   }

   private ClientSession createSessionInternal(final String username,
                                               final String password,
                                               final boolean xa,
                                               final boolean autoCommitSends,
                                               final boolean autoCommitAcks,
                                               final boolean preAcknowledge,
                                               final int ackBatchSize) throws HornetQException
   {
      synchronized (createSessionLock)
      {
         String name = UUIDGenerator.getInstance().generateStringUUID();

         boolean retry = false;
         do
         {
            Version clientVersion = VersionLoader.getVersion();

            Lock lock = null;

            try
            {
               Channel channel1;

               synchronized (failoverLock)
               {
                  if (connection == null)
                  {
                     throw new IllegalStateException("Connection is null");
                  }

                  channel1 = connection.getChannel(1, -1);

                  // Lock it - this must be done while the failoverLock is held
                  channel1.getLock().lock();

                  lock = channel1.getLock();
               } // We can now release the failoverLock

               // We now set a flag saying createSession is executing
               synchronized (exitLock)
               {
                  inCreateSession = true;
               }

               long sessionChannelID = connection.generateChannelID();

               Packet request = new CreateSessionMessage(name,
                                                         sessionChannelID,
                                                         clientVersion.getIncrementingVersion(),
                                                         username,
                                                         password,
                                                         serverLocator.getMinLargeMessageSize(),
                                                         xa,
                                                         autoCommitSends,
                                                         autoCommitAcks,
                                                         preAcknowledge,
                                                         serverLocator.getConfirmationWindowSize(),
                                                         null);

               CreateSessionResponseMessage response;
               try
               {
                  response = (CreateSessionResponseMessage) channel1.sendBlocking(request, PacketImpl.CREATESESSION_RESP);
               }
               catch (HornetQException e)
               {
                  if (e.getCode() == HornetQException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS)
                  {
                     connection.destroy();
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

               Channel sessionChannel = connection.getChannel(sessionChannelID,
                                                              serverLocator.getConfirmationWindowSize());

               ClientSessionInternal session = new ClientSessionImpl(this,
                                                                     name,
                                                                     username,
                                                                     password,
                                                                     xa,
                                                                     autoCommitSends,
                                                                     autoCommitAcks,
                                                                     preAcknowledge,
                                                                     serverLocator.isBlockOnAcknowledge(),
                                                                     serverLocator.isAutoGroup(),
                                                                     ackBatchSize,
                                                                     serverLocator.getConsumerWindowSize(),
                                                                     serverLocator.getConsumerMaxRate(),
                                                                     serverLocator.getConfirmationWindowSize(),
                                                                     serverLocator.getProducerWindowSize(),
                                                                     serverLocator.getProducerMaxRate(),
                                                                     serverLocator.isBlockOnNonDurableSend(),
                                                                     serverLocator.isBlockOnDurableSend(),
                                                                     serverLocator.isCacheLargeMessagesClient(),
                                                                     serverLocator.getMinLargeMessageSize(),
                                                                     serverLocator.isCompressLargeMessage(),
                                                                     serverLocator.getInitialMessagePacketSize(),
                                                                     serverLocator.getGroupID(),
                                                                     connection,
                                                                     response.getServerVersion(),
                                                                     sessionChannel,
                                                                     orderedExecutorFactory.getExecutor(),
                                                                     orderedExecutorFactory.getExecutor());

               synchronized (sessions)
               {
                  sessions.add(session);
               }

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
      throw new IllegalStateException("Internal Error! ClientSessionFactoryImpl::createSessionInternal " + "just reached a condition that was not supposed to happen. "
                                      + "Please inform this condition to the HornetQ team");
   }

   private void callFailureListeners(final HornetQException me, final boolean afterReconnect, final boolean failedOver)
   {
      final List<SessionFailureListener> listenersClone = new ArrayList<SessionFailureListener>(listeners);

      for (final SessionFailureListener listener : listenersClone)
      {
         try
         {
            if (afterReconnect)
            {
               listener.connectionFailed(me, failedOver);
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
            ClientSessionFactoryImpl.log.error("Failed to execute failure listener", t);
         }
      }
   }

   /*
    * Re-attach sessions all pre-existing sessions to the new remoting connection
    */
   private void reconnectSessions(final CoreRemotingConnection oldConnection, final int reconnectAttempts)
   {
      HashSet<ClientSessionInternal> sessionsToFailover;
      synchronized (sessions)
      {
         sessionsToFailover = new HashSet<ClientSessionInternal>(sessions);
      }

      for (ClientSessionInternal session : sessionsToFailover)
      {
         session.preHandleFailover(connection);
      }

      getConnectionWithRetry(reconnectAttempts);

      if (connection == null)
      {
         ClientSessionFactoryImpl.log.warn("Failed to connect to server.");

         return;
      }

      List<FailureListener> oldListeners = oldConnection.getFailureListeners();

      List<FailureListener> newListeners = new ArrayList<FailureListener>(connection.getFailureListeners());

      for (FailureListener listener : oldListeners)
      {
         // Add all apart from the first one which is the old DelegatingFailureListener

         if (listener instanceof DelegatingFailureListener == false)
         {
            newListeners.add(listener);
         }
      }

      connection.setFailureListeners(newListeners);

      for (ClientSessionInternal session : sessionsToFailover)
      {
         session.handleFailover(connection);
      }
   }

   private void getConnectionWithRetry(final int reconnectAttempts)
   {
      if (ClientSessionFactoryImpl.log.isTraceEnabled())
      {
         ClientSessionFactoryImpl.log.trace("getConnectionWithRetry::" + reconnectAttempts +
                                            " with retryInterval = " +
                                            retryInterval +
                                            " multiplier = " +
                                            retryIntervalMultiplier, new Exception("trace"));
      }

      long interval = retryInterval;

      int count = 0;

      synchronized (waitLock)
      {
         while (!exitLoop)
         {
            if (ClientSessionFactoryImpl.isDebug)
            {
               ClientSessionFactoryImpl.log.debug("Trying reconnection attempt " + count + "/" + reconnectAttempts);
            }

            getConnection();

            if (connection == null)
            {
               // Failed to get connection

               if (reconnectAttempts != 0)
               {
                  count++;

                  if (reconnectAttempts != -1 && count == reconnectAttempts)
                  {
                     if (reconnectAttempts != 1)
                     {
                        ClientSessionFactoryImpl.log.warn("Tried " + reconnectAttempts +
                                                          " times to connect. Now giving up on reconnecting it.");
                     }
                     else if (reconnectAttempts == 1)
                     {
                        ClientSessionFactoryImpl.log.debug("Trying to connect towards " + this);
                     }

                     return;
                  }

                  if (ClientSessionFactoryImpl.isTrace)
                  {
                     ClientSessionFactoryImpl.log.trace("Waiting " + interval +
                                                        " milliseconds before next retry. RetryInterval=" +
                                                        retryInterval +
                                                        " and multiplier = " +
                                                        retryIntervalMultiplier);
                  }

                  try
                  {
                     waitLock.wait(interval);
                  }
                  catch (InterruptedException interrupted)
                  {
                     throw new HornetQInterruptedException(interrupted);
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
                  ClientSessionFactoryImpl.log.debug("Could not connect to any server. Didn't have reconnection configured on the ClientSessionFactory");
                  return;
               }
            }
            else
            {
               if (log.isDebugEnabled())
               {
                  log.debug("Reconnection successfull");
               }
               return;
            }
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

            connector = connectorFactory.createConnector(connectorConfig.getParams(),
                                                         handler,
                                                         this,
                                                         closeExecutor,
                                                         threadPool,
                                                         scheduledThreadPool);

            if (ClientSessionFactoryImpl.log.isDebugEnabled())
            {
               ClientSessionFactoryImpl.log.debug("Trying to connect with connector = " + connectorFactory +
                                                  ", parameters = " +
                                                  connectorConfig.getParams() +
                                                  " connector = " +
                                                  connector);
            }

            if (connector != null)
            {
               connector.start();

               if (ClientSessionFactoryImpl.isDebug)
               {
                  ClientSessionFactoryImpl.log.debug("Trying to connect at the main server using connector :" + connectorConfig);
               }

               tc = connector.createConnection();

               if (tc == null)
               {
                  if (ClientSessionFactoryImpl.isDebug)
                  {
                     ClientSessionFactoryImpl.log.debug("Main server is not up. Hopefully there's a backup configured now!");
                  }

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
            // if connection fails we can try the backup in case it has come live
            if (connector == null)
            {
               if (backupConfig != null)
               {
                  if (ClientSessionFactoryImpl.isDebug)
                  {
                     ClientSessionFactoryImpl.log.debug("Trying backup config = " + backupConfig);
                  }
                  ConnectorFactory backupConnectorFactory = instantiateConnectorFactory(backupConfig.getFactoryClassName());
                  connector = backupConnectorFactory.createConnector(backupConfig.getParams(),
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
                        if (ClientSessionFactoryImpl.isDebug)
                        {
                           ClientSessionFactoryImpl.log.debug("Backup is not active yet");
                        }

                        try
                        {
                           connector.close();
                        }
                        catch (Throwable t)
                        {
                        }

                        connector = null;
                     }
                     else
                     {
                        /*looks like the backup is now live, lets use that*/

                        if (ClientSessionFactoryImpl.isDebug)
                        {
                           ClientSessionFactoryImpl.log.debug("Connected to the backup at " + backupConfig);
                        }

                        connectorConfig = backupConfig;

                        backupConfig = null;

                        connectorFactory = backupConnectorFactory;
                     }
                  }
               }
               else
               {
                  if (ClientSessionFactoryImpl.isTrace)
                  {
                     ClientSessionFactoryImpl.log.trace("No Backup configured!", new Exception("trace"));
                  }
               }
            }
         }
         catch (Exception e)
         {
            // Sanity catch for badly behaved remoting plugins

            ClientSessionFactoryImpl.log.warn("connector.create or connectorFactory.createConnector should never throw an exception, implementation is badly behaved, but we'll deal with it anyway.",
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
            if (ClientSessionFactoryImpl.isTrace)
            {
               ClientSessionFactoryImpl.log.trace("returning connection = " + connection + " as tc == null");
            }
            return connection;
         }

         connection = new RemotingConnectionImpl(tc, callTimeout, interceptors);

         connection.addFailureListener(new DelegatingFailureListener(connection.getID()));

         Channel channel0 = connection.getChannel(0, -1);

         channel0.setHandler(new Channel0Handler(connection));

         if (clientFailureCheckPeriod != -1)
         {
            if (pingerFuture == null)
            {
               pingRunnable = new PingRunnable();

               pingerFuture = scheduledThreadPool.scheduleWithFixedDelay(new ActualScheduledPinger(pingRunnable),
                                                                         0,
                                                                         clientFailureCheckPeriod,
                                                                         TimeUnit.MILLISECONDS);
               // To make sure the first ping will be sent
               pingRunnable.send();
            }
            // send a ping every time we create a new remoting connection
            // to set up its TTL on the server side
            else
            {
               pingRunnable.run();
            }
         }

         if (serverLocator.getTopology() != null)
         {
            if (ClientSessionFactoryImpl.isTrace)
            {
               ClientSessionFactoryImpl.log.trace(this + "::Subscribing Topology");
            }

            channel0.send(new SubscribeClusterTopologyUpdatesMessageV2(serverLocator.isClusterConnection(),
                                                                       VersionLoader.getVersion()
                                                                                    .getIncrementingVersion()));

         }
      }

      if (serverLocator.getAfterConnectInternalListener() != null)
      {
         serverLocator.getAfterConnectInternalListener().onConnection(this);
      }

      if (ClientSessionFactoryImpl.log.isTraceEnabled())
      {
         ClientSessionFactoryImpl.log.trace("returning " + connection);
      }

      return connection;
   }

   /**
    * @param channel0
    */
   public void sendNodeAnnounce(final long currentEventID,
                                String nodeID,
                                boolean isBackup,
                                TransportConfiguration config,
                                TransportConfiguration backupConfig)
   {
      Channel channel0 = connection.getChannel(0, -1);
      if (ClientSessionFactoryImpl.isDebug)
      {
         ClientSessionFactoryImpl.log.debug("Announcing node " + serverLocator.getNodeID() + ", isBackup=" + isBackup);
      }
      channel0.send(new NodeAnnounceMessage(currentEventID, nodeID, isBackup, config, backupConfig));
   }

   @Override
   public void finalize() throws Throwable
   {
      if (!closed && finalizeCheck)
      {
         ClientSessionFactoryImpl.log.warn("I'm closing a core ClientSessionFactory you left open. Please make sure you close all ClientSessionFactories explicitly " + "before letting them go out of scope! " +
                                           System.identityHashCode(this));

         ClientSessionFactoryImpl.log.warn("The ClientSessionFactory you didn't close was created here:", e);

         close();
      }

      super.finalize();
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
                                                            "\"",
                                                   e);
             }
         }
      });
   }

   private void lockChannel1()
   {
      if (connection != null)
      {
         Channel channel1 = connection.getChannel(1, -1);

         if (channel1 != null)
         {
            channel1.getLock().lock();
         }
      }
   }

   private void unlockChannel1()
   {
      if (connection != null)
      {
         Channel channel1 = connection.getChannel(1, -1);

         if (channel1 != null)
         {
            channel1.getLock().unlock();
         }
      }
   }

   private void forceReturnChannel1()
   {
      if (connection != null)
      {
         Channel channel1 = connection.getChannel(1, -1);

         if (channel1 != null)
         {
            channel1.returnBlocking();
         }
      }
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
            final DisconnectMessage msg = (DisconnectMessage)packet;

            SimpleString nodeID = msg.getNodeID();

            if (ClientSessionFactoryImpl.log.isTraceEnabled())
            {
               ClientSessionFactoryImpl.log.trace("Disconnect being called on client:" + msg +
                                                  " server locator = " +
                                                  serverLocator +
                                                  " notifying node " +
                                                  nodeID +
                                                  " as down", new Exception("trace"));
            }

            if (nodeID != null)
            {
               serverLocator.notifyNodeDown(System.currentTimeMillis(), msg.getNodeID().toString());
            }

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
         else if (type == PacketImpl.CLUSTER_TOPOLOGY)
         {
            ClusterTopologyChangeMessage topMessage = (ClusterTopologyChangeMessage)packet;
            notifyTopologyChange(topMessage);
         }
         else if (type == PacketImpl.CLUSTER_TOPOLOGY_V2)
         {
            ClusterTopologyChangeMessage_V2 topMessage = (ClusterTopologyChangeMessage_V2)packet;
            notifyTopologyChange(topMessage);
         }
      }

      /**
       * @param topMessage
       */
      private void notifyTopologyChange(final ClusterTopologyChangeMessage topMessage)
      {
         threadPool.execute(new Runnable()
         {
            public void run()
            {
               final long eventUID;
               if (topMessage instanceof ClusterTopologyChangeMessage_V2)
                  eventUID = ((ClusterTopologyChangeMessage_V2)topMessage).getUniqueEventID();
               else
                  eventUID = System.currentTimeMillis();

               if (topMessage.isExit())
               {
                  if (ClientSessionFactoryImpl.isDebug)
                  {
                     ClientSessionFactoryImpl.log.debug("Notifying " + topMessage.getNodeID() + " going down");
                  }
                  serverLocator.notifyNodeDown(eventUID, topMessage.getNodeID());
                  return;
               }
               if (ClientSessionFactoryImpl.isDebug)
               {
                  ClientSessionFactoryImpl.log.debug("Node " + topMessage.getNodeID() +
                                                     " going up, connector = " +
                                                     topMessage.getPair() +
                                                     ", isLast=" +
                                                     topMessage.isLast() +
                                                     " csf created at\nserverLocator=" +
                                                     serverLocator, e);
               }

               Pair<TransportConfiguration, TransportConfiguration> transportConfig = topMessage.getPair();
               if (transportConfig.getA() == null && transportConfig.getB() == null)
               {
                  transportConfig = new Pair<TransportConfiguration, TransportConfiguration>(conn.getTransportConnection()
                                                                                                 .getConnectorConfig(),
                                                                                             null);
               }

               serverLocator.notifyNodeUp(eventUID, topMessage.getNodeID(), transportConfig, topMessage.isLast());
            }
         });
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

      public void connectionFailed(final HornetQException me, final boolean failedOver)
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

         if (clientFailureCheckPeriod != -1 && connectionTTL != -1 && now >= lastCheck + connectionTTL)
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

         send();
      }

      /**
       *
       */
      public void send()
      {
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

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "ClientSessionFactoryImpl [serverLocator=" + serverLocator +
             ", connectorConfig=" +
             connectorConfig +
             ", backupConfig=" +
             backupConfig +
             "]";
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.client.impl.ClientSessionFactoryInternal#setReconnectAttempts(int)
    */
   public void setReconnectAttempts(final int attempts)
   {
      reconnectAttempts = attempts;
   }

   public Object getConnector()
   {
      return connector;
   }
}
