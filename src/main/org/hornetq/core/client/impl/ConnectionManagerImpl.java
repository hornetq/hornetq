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

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.EARLY_RESPONSE;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.ChannelHandler;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.AbstractBufferHandler;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.Ping;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.core.remoting.spi.Connector;
import org.hornetq.core.remoting.spi.ConnectorFactory;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.core.version.Version;
import org.hornetq.utils.ConcurrentHashSet;
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
 *
 */
public class ConnectionManagerImpl implements ConnectionManager, ConnectionLifeCycleListener
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(ConnectionManagerImpl.class);

   // Attributes
   // -----------------------------------------------------------------------------------

   private final ClientSessionFactory sessionFactory;

   private final TransportConfiguration connectorConfig;

   private final TransportConfiguration backupConfig;

   private ConnectorFactory connectorFactory;

   private Map<String, Object> transportParams;

   private ConnectorFactory backupConnectorFactory;

   private Map<String, Object> backupTransportParams;

   private final int maxConnections;

   private final long callTimeout;

   private final long clientFailureCheckPeriod;

   private final long connectionTTL;

   private final Map<ClientSessionInternal, RemotingConnection> sessions = new HashMap<ClientSessionInternal, RemotingConnection>();

   private final Object exitLock = new Object();

   private final Object createSessionLock = new Object();

   private boolean inCreateSession;

   private final Object failoverLock = new Object();

   private final ExecutorFactory orderedExecutorFactory;

   private final ExecutorService threadPool;

   private final ScheduledExecutorService scheduledThreadPool;

   private final Map<Object, ConnectionEntry> connections = Collections.synchronizedMap(new LinkedHashMap<Object, ConnectionEntry>());

   private int refCount;

   private Iterator<ConnectionEntry> mapIterator;

   private final long retryInterval;

   private final double retryIntervalMultiplier; // For exponential backoff

   private final int reconnectAttempts;

   private boolean failoverOnServerShutdown;

   private Set<FailureListener> listeners = new ConcurrentHashSet<FailureListener>();

   private Connector connector;

   private Future<?> pingerFuture;

   private PingRunnable pingRunnable;
   
   private volatile boolean exitLoop;

   // debug

   private static Map<TransportConfiguration, Set<RemotingConnection>> debugConns;

   private static boolean debug = false;

   public static void enableDebug()
   {
      debug = true;

      debugConns = new ConcurrentHashMap<TransportConfiguration, Set<RemotingConnection>>();
   }

   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   public ConnectionManagerImpl(final ClientSessionFactory sessionFactory,
                                final TransportConfiguration connectorConfig,
                                final TransportConfiguration backupConfig,
                                final boolean failoverOnServerShutdown,
                                final int maxConnections,
                                final long callTimeout,
                                final long clientFailureCheckPeriod,
                                final long connectionTTL,
                                final long retryInterval,
                                final double retryIntervalMultiplier,
                                final int reconnectAttempts,
                                final ExecutorService threadPool,
                                final ScheduledExecutorService scheduledThreadPool)
   {
      this.sessionFactory = sessionFactory;

      this.connectorConfig = connectorConfig;

      this.backupConfig = backupConfig;

      this.failoverOnServerShutdown = failoverOnServerShutdown;

      connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());

      transportParams = connectorConfig.getParams();

      if (backupConfig != null)
      {
         backupConnectorFactory = instantiateConnectorFactory(backupConfig.getFactoryClassName());

         backupTransportParams = backupConfig.getParams();
      }
      else
      {
         backupConnectorFactory = null;

         backupTransportParams = null;
      }

      this.maxConnections = maxConnections;

      this.callTimeout = callTimeout;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.reconnectAttempts = reconnectAttempts;

      this.scheduledThreadPool = scheduledThreadPool;

      this.threadPool = threadPool;

      this.orderedExecutorFactory = new OrderedExecutorFactory(threadPool);
   }

   // ConnectionLifeCycleListener implementation --------------------------------------------------

   public void connectionCreated(final Connection connection)
   {
   }

   public void connectionDestroyed(final Object connectionID)
   {
      failConnection(connectionID, new MessagingException(MessagingException.NOT_CONNECTED, "Channel disconnected"));
   }

   public void connectionException(final Object connectionID, final MessagingException me)
   {
      failConnection(connectionID, me);
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
                                      final int producerWindowSize,
                                      final int consumerWindowSize,
                                      final int producerMaxRate,
                                      final int consumerMaxRate,
                                      final boolean blockOnNonPersistentSend,
                                      final boolean blockOnPersistentSend) throws MessagingException
   {
      synchronized (createSessionLock)
      {
         String name = UUIDGenerator.getInstance().generateSimpleStringUUID().toString();

         boolean retry = false;
         do
         {
            Version clientVersion = VersionLoader.getVersion();

            RemotingConnection connection = null;

            Lock lock = null;

            try
            {
               Channel channel1;

               synchronized (failoverLock)
               {
                  connection = getConnectionWithRetry(1, reconnectAttempts);

                  if (connection == null)
                  {
                     if (exitLoop)
                     {                        
                        return null;
                     }
                     // This can happen if the connection manager gets exitLoop - e.g. the server gets shut down

                     throw new MessagingException(MessagingException.NOT_CONNECTED,
                                                  "Unable to connect to server using configuration " + connectorConfig);
                  }

                  channel1 = connection.getChannel(1, -1, false);

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
                                                         minLargeMessageSize,
                                                         xa,
                                                         autoCommitSends,
                                                         autoCommitAcks,
                                                         preAcknowledge,
                                                         producerWindowSize);

               Packet pResponse = channel1.sendBlocking(request);

               if (pResponse.getType() == EARLY_RESPONSE)
               {
                  // This means the thread was blocked on create session and failover unblocked it
                  // so failover could occur

                  // So we just need to return our connections and flag for retry

                  returnConnection(connection.getID());

                  retry = true;
               }
               else
               {
                  CreateSessionResponseMessage response = (CreateSessionResponseMessage)pResponse;

                  Channel sessionChannel = connection.getChannel(sessionChannelID,
                                                                 producerWindowSize,
                                                                 producerWindowSize != -1);

                  ClientSessionInternal session = new ClientSessionImpl(this,
                                                                        name,
                                                                        xa,
                                                                        autoCommitSends,
                                                                        autoCommitAcks,
                                                                        preAcknowledge,
                                                                        blockOnAcknowledge,
                                                                        autoGroup,
                                                                        ackBatchSize,
                                                                        consumerWindowSize,
                                                                        consumerMaxRate,
                                                                        producerMaxRate,
                                                                        blockOnNonPersistentSend,
                                                                        blockOnPersistentSend,
                                                                        cacheLargeMessageClient,
                                                                        minLargeMessageSize,
                                                                        connection,
                                                                        response.getServerVersion(),
                                                                        sessionChannel,
                                                                        orderedExecutorFactory.getExecutor());

                  sessions.put(session, connection);

                  ChannelHandler handler = new ClientSessionPacketHandler(session, sessionChannel);

                  sessionChannel.setHandler(handler);

                  return new DelegatingSession(session);
               }
            }
            catch (Throwable t)
            {
               if (lock != null)
               {
                  lock.unlock();

                  lock = null;
               }

               if (connection != null)
               {
                  returnConnection(connection.getID());
               }

               if (t instanceof MessagingException)
               {
                  throw (MessagingException)t;
               }
               else
               {
                  MessagingException me = new MessagingException(MessagingException.INTERNAL_ERROR,
                                                                 "Failed to create session");

                  me.initCause(t);

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

            returnConnection(session.getConnection().getID());
         }
      }
   }

   public synchronized int numConnections()
   {
      return connections.size();
   }

   public int numSessions()
   {
      return sessions.size();
   }

   public void addFailureListener(FailureListener listener)
   {
      listeners.add(listener);
   }

   public boolean removeFailureListener(FailureListener listener)
   {
      return listeners.remove(listener);
   }
   
   
   
   public void causeExit()
   {
      exitLoop = true;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   private volatile boolean stopPingingAfterOne;

   public void stopPingingAfterOne()
   {
      this.stopPingingAfterOne = true;
   }

   // Protected
   // ------------------------------------------------------------------------------------

   // Package Private
   // ------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void handleConnectionFailure(final MessagingException me, final Object connectionID)
   {
      failoverOrReconnect(me, connectionID);
   }

   private void failoverOrReconnect(final MessagingException me, final Object connectionID)
   {
      boolean done = false;

      synchronized (failoverLock)
      {
         if (connectionID != null && !connections.containsKey(connectionID))
         {
            // We already failed over/reconnected - probably the first failure came in, all the connections were failed
            // over then a async connection exception or disconnect
            // came in for one of the already exitLoop connections, so we return true - we don't want to call the
            // listeners again

            return;
         }

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
         // It should catch MessagingException.INTERRUPTED in the call to channel.sendBlocking
         // It should then return its connections, with channel 1 lock still held
         // It can then release the channel 1 lock, and retry (which will cause locking on failoverLock
         // until failover is complete

         boolean serverShutdown = me.getCode() == MessagingException.DISCONNECTED;

         boolean attemptFailoverOrReconnect = (backupConnectorFactory != null || reconnectAttempts != 0)
                                                && (failoverOnServerShutdown || !serverShutdown);
         
         log.info("Attempting failover or reconnect " + attemptFailoverOrReconnect);

         if (attemptFailoverOrReconnect)
         {
            lockAllChannel1s();

            final boolean needToInterrupt;

            synchronized (exitLock)
            {
               needToInterrupt = inCreateSession;
            }

            unlockAllChannel1s();

            if (needToInterrupt)
            {
               // Forcing return all channels won't guarantee that any blocked thread will return immediately
               // So we need to wait for it
               forceReturnAllChannel1s();

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

            Set<RemotingConnection> oldConnections = new HashSet<RemotingConnection>();

            for (ConnectionEntry entry : connections.values())
            {
               oldConnections.add(entry.connection);
            }

            connections.clear();

            refCount = 0;

            mapIterator = null;

            try
            {
               connector.close();
            }
            catch (Exception ignore)
            {
            }

            connector = null;

            if (backupConnectorFactory != null)
            {
               // Now try failing over to backup

               connectorFactory = backupConnectorFactory;

               transportParams = backupTransportParams;

               backupConnectorFactory = null;

               backupTransportParams = null;

               done = reattachSessions(reconnectAttempts == -1 ? -1 : reconnectAttempts + 1);
            }
            else if (reconnectAttempts != 0)
            {
               done = reattachSessions(reconnectAttempts);
            }

            if (done)
            {
               // Destroy the old connections
               for (RemotingConnection connection : oldConnections)
               {
                  connection.destroy();
               }
            }
            else
            {
               for (RemotingConnection connection : oldConnections)
               {
                  connection.destroy();
               }

               closeConnectionsAndCallFailureListeners(me);
            }
         }
         else
         {
            log.info("Just closing connections and calling failure listeners");
            
            closeConnectionsAndCallFailureListeners(me);
         }
      }
   }

   private void closeConnectionsAndCallFailureListeners(final MessagingException me)
   {
      refCount = 0;
      mapIterator = null;
      checkCloseConnections();

      // TODO (after beta5) should really execute on different thread then remove the async in HornetQConnection

      // threadPool.execute(new Runnable()
      // {
      // public void run()
      // {
      callFailureListeners(me);
      // }
      // });
   }

   private void callFailureListeners(final MessagingException me)
   {
      final List<FailureListener> listenersClone = new ArrayList<FailureListener>(listeners);

      for (final FailureListener listener : listenersClone)
      {
         try
         {
            listener.connectionFailed(me);
         }
         catch (final Throwable t)
         {
            // Failure of one listener to execute shouldn't prevent others
            // from
            // executing
            log.error("Failed to execute failure listener", t);
         }
      }
   }

   /*
    * Re-attach sessions all pre-existing sessions to new remoting connections
    */
   private boolean reattachSessions(final int reconnectAttempts)
   {
      // We re-attach sessions per connection to ensure there is the same mapping of channel id
      // on live and backup connections

      Map<RemotingConnection, List<ClientSessionInternal>> sessionsPerConnection = new HashMap<RemotingConnection, List<ClientSessionInternal>>();

      for (Map.Entry<ClientSessionInternal, RemotingConnection> entry : sessions.entrySet())
      {
         ClientSessionInternal session = entry.getKey();

         RemotingConnection connection = entry.getValue();

         List<ClientSessionInternal> sessions = sessionsPerConnection.get(connection);

         if (sessions == null)
         {
            sessions = new ArrayList<ClientSessionInternal>();

            sessionsPerConnection.put(connection, sessions);
         }

         sessions.add(session);
      }

      boolean ok = true;

      for (Map.Entry<RemotingConnection, List<ClientSessionInternal>> entry : sessionsPerConnection.entrySet())
      {
         List<ClientSessionInternal> theSessions = entry.getValue();

         RemotingConnection backupConnection = getConnectionWithRetry(theSessions.size(), reconnectAttempts);

         if (backupConnection == null)
         {
            log.warn("Failed to connect to server.");

            ok = false;

            break;
         }

         List<FailureListener> oldListeners = entry.getKey().getFailureListeners();

         List<FailureListener> newListeners = new ArrayList<FailureListener>(backupConnection.getFailureListeners());

         for (FailureListener listener : oldListeners)
         {
            // Add all apart from the first one which is the old DelegatingFailureListener

            if (listener instanceof DelegatingFailureListener == false)
            {
               newListeners.add(listener);
            }
         }

         backupConnection.setFailureListeners(newListeners);

         for (ClientSessionInternal session : theSessions)
         {
            sessions.put(session, backupConnection);
         }
      }

      if (ok)
      {
         // If all connections got ok, then handle failover
         for (Map.Entry<ClientSessionInternal, RemotingConnection> entry : sessions.entrySet())
         {
            boolean b = entry.getKey().handleFailover(entry.getValue());

            if (!b)
            {
               // If a session fails to re-attach we doom the lot, but we make sure we try all sessions and don't exit
               // early
               // or connections might be left lying around
               ok = false;
            }
         }
      }

      return ok;
   }

   private RemotingConnection getConnectionWithRetry(final int initialRefCount, final int reconnectAttempts)
   {
      long interval = retryInterval;

      int count = 0;

      while (true)
      {
         if (exitLoop)
         {
            return null;
         }
         
         RemotingConnection connection = getConnection(initialRefCount);

         if (connection == null)
         {
            // Failed to get connection

            if (reconnectAttempts != 0)
            {
               count++;

               if (reconnectAttempts != -1 && count == reconnectAttempts)
               {
                  log.warn("Tried " + reconnectAttempts + " times to connect. Now giving up.");

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
               interval *= retryIntervalMultiplier;
            }
            else
            {
               return null;
            }
         }
         else
         {
            return connection;
         }
      }
   }

   private void checkCloseConnections()
   {
      if (refCount == 0)
      {
         // Close connections

         Set<ConnectionEntry> copy = new HashSet<ConnectionEntry>(connections.values());

         if (pingerFuture != null)
         {
            pingRunnable.cancel();

            boolean ok = pingerFuture.cancel(false);

            pingRunnable = null;

            pingerFuture = null;
         }

         connections.clear();

         for (ConnectionEntry entry : copy)
         {
            try
            {
               entry.connection.destroy();
            }
            catch (Throwable ignore)
            {
            }
         }

         mapIterator = null;

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

   public RemotingConnection getConnection(final int initialRefCount)
   {
      RemotingConnection conn;

      if (connections.size() < maxConnections)
      {
         // Create a new one

         Connection tc = null;

         try
         {
            if (connector == null)
            {
               DelegatingBufferHandler handler = new DelegatingBufferHandler();

               connector = connectorFactory.createConnector(transportParams,
                                                            handler,
                                                            this,
                                                            threadPool,
                                                            scheduledThreadPool);

               if (connector != null)
               {
                  connector.start();
               }
            }

            if (connector != null)
            {
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

            log.warn("connector.create or connectorFactory.createConnector should never throw an exception, implementation is badly behaved, but we'll deal with it anyway.",
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
            return null;
         }

         conn = new RemotingConnectionImpl(tc, callTimeout, null);

         conn.addFailureListener(new DelegatingFailureListener(conn.getID()));

         conn.getChannel(0, -1, false).setHandler(new Channel0Handler(conn));

         connections.put(conn.getID(), new ConnectionEntry(conn,
                                                           connector,
                                                           clientFailureCheckPeriod,
                                                           System.currentTimeMillis()));

         if (clientFailureCheckPeriod != -1 && pingerFuture == null)
         {
            pingRunnable = new PingRunnable();

            pingerFuture = scheduledThreadPool.scheduleWithFixedDelay(new ActualScheduled(pingRunnable),
                                                                      0,
                                                                      clientFailureCheckPeriod,
                                                                      TimeUnit.MILLISECONDS);
         }

         if (debug)
         {
            checkAddDebug(conn);
         }
      }
      else
      {
         // Return one round-robin from the list

         if (mapIterator == null || !mapIterator.hasNext())
         {
            mapIterator = connections.values().iterator();
         }

         ConnectionEntry entry = mapIterator.next();

         conn = entry.connection;
      }

      refCount += initialRefCount;

      return conn;
   }

   private void returnConnection(final Object connectionID)
   {
      ConnectionEntry entry = connections.get(connectionID);

      if (refCount != 0)
      {
         refCount--;
      }

      if (entry != null)
      {
         checkCloseConnections();
      }
      else
      {
         // Can be legitimately null if session was exitLoop before then went to remove session from csf
         // and locked since failover had started then after failover removes it but it's already been failed
      }
   }

   private ConnectorFactory instantiateConnectorFactory(final String connectorFactoryClassName)
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

   private void lockAllChannel1s()
   {
      for (ConnectionEntry entry : connections.values())
      {
         Channel channel1 = entry.connection.getChannel(1, -1, false);

         channel1.getLock().lock();
      }
   }

   private void unlockAllChannel1s()
   {
      for (ConnectionEntry entry : connections.values())
      {
         Channel channel1 = entry.connection.getChannel(1, -1, false);

         channel1.getLock().unlock();
      }
   }

   private void forceReturnAllChannel1s()
   {
      for (ConnectionEntry entry : connections.values())
      {
         Channel channel1 = entry.connection.getChannel(1, -1, false);

         channel1.returnBlocking();
      }
   }

   private void failConnection(final Object connectionID, final MessagingException me)
   {
      ConnectionEntry entry = connections.get(connectionID);

      if (entry != null)
      {
         RemotingConnection conn = entry.connection;

         conn.fail(me);
      }
   }

   private class Channel0Handler implements ChannelHandler
   {
      private final RemotingConnection conn;

      private Channel0Handler(final RemotingConnection conn)
      {
         this.conn = conn;
      }

      public void handlePacket(final Packet packet)
      {
         final byte type = packet.getType();

         if (type == PacketImpl.DISCONNECT)
         {
            log.info("Got a disconnect message");
            threadPool.execute(new Runnable()
            {
               // Must be executed on new thread since cannot block the netty thread for a long time and fail can
               // cause reconnect loop
               public void run()
               {
                  conn.fail(new MessagingException(MessagingException.DISCONNECTED,
                                                   "The connection was exitLoop by the server"));
               }
            });
         }
      }
   }

   private static class ConnectionEntry
   {
      ConnectionEntry(final RemotingConnection connection,
                      final Connector connector,
                      final long expiryPeriod,
                      final long createTime)
      {
         this.connection = connection;

         this.connector = connector;

         this.expiryPeriod = expiryPeriod;

         this.lastCheck = createTime;
      }

      final RemotingConnection connection;

      final Connector connector;

      volatile long lastCheck;

      final long expiryPeriod;
   }

   private class DelegatingBufferHandler extends AbstractBufferHandler
   {
      public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
      {
         ConnectionEntry entry = connections.get(connectionID);

         if (entry != null)
         {
            entry.connection.bufferReceived(connectionID, buffer);
         }
      }
   }

   private class DelegatingFailureListener implements FailureListener
   {
      final Object connectionID;

      DelegatingFailureListener(final Object connectionID)
      {
         this.connectionID = connectionID;
      }

      public void connectionFailed(final MessagingException me)
      {
         handleConnectionFailure(me, connectionID);
      }
   }

   // Debug only

   private void checkAddDebug(final RemotingConnection conn)
   {
      Set<RemotingConnection> conns;

      synchronized (debugConns)
      {
         conns = debugConns.get(connectorConfig);

         if (conns == null)
         {
            conns = new HashSet<RemotingConnection>();

            debugConns.put(connectorConfig, conns);
         }

         conns.add(conn);
      }
   }

   public static void failAllConnectionsForConnector(final TransportConfiguration config)
   {
      Set<RemotingConnection> conns;

      synchronized (debugConns)
      {
         conns = debugConns.get(config);

         if (conns != null)
         {
            conns = new HashSet<RemotingConnection>(debugConns.get(config));
         }
      }

      if (conns != null)
      {
         for (RemotingConnection conn : conns)
         {
            conn.fail(new MessagingException(MessagingException.INTERNAL_ERROR, "blah"));
         }
      }
   }

   private static final class ActualScheduled implements Runnable
   {
      private final WeakReference<PingRunnable> pingRunnable;

      ActualScheduled(final PingRunnable runnable)
      {
         this.pingRunnable = new WeakReference<PingRunnable>(runnable);
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

      public synchronized void run()
      {
         if (cancelled || (stopPingingAfterOne && !first))
         {
            return;
         }

         first = false;

         synchronized (connections)
         {
            long now = System.currentTimeMillis();

            for (ConnectionEntry entry : connections.values())
            {
               final RemotingConnection connection = entry.connection;

               if (entry.expiryPeriod != -1 && now >= entry.lastCheck + entry.expiryPeriod)
               {
                  if (!connection.checkDataReceived())
                  {
                     final MessagingException me = new MessagingException(MessagingException.CONNECTION_TIMEDOUT,
                                                                          "Did not receive data from server for " + connection.getTransportConnection());

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
                     entry.lastCheck = now;
                  }
               }

               // Send a ping

               Ping ping = new Ping(connectionTTL);

               Channel channel0 = connection.getChannel(0, -1, false);

               channel0.send(ping);
            }
         }
      }

      public synchronized void cancel()
      {
         cancelled = true;
      }
   }

}
