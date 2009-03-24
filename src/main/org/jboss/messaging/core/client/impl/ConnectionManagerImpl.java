/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.EARLY_RESPONSE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.locks.Lock;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.AbstractBufferHandler;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.Connector;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.utils.UUIDGenerator;
import org.jboss.messaging.utils.VersionLoader;

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

   private final TransportConfiguration connectorConfig;

   private final TransportConfiguration backupConfig;

   private ConnectorFactory connectorFactory;

   private Map<String, Object> transportParams;

   private ConnectorFactory backupConnectorFactory;

   private Map<String, Object> backupTransportParams;

   private final int maxConnections;

   private final long callTimeout;

   private final long pingPeriod;

   private final long connectionTTL;

   private final Map<ClientSessionInternal, RemotingConnection> sessions = new HashMap<ClientSessionInternal, RemotingConnection>();

   private final Object exitLock = new Object();

   private final Object createSessionLock = new Object();

   private boolean inCreateSession;

   private final Object failoverLock = new Object();

   // TODO - allow this to be configurable
   private static final ScheduledThreadPoolExecutor pingExecutor = new ScheduledThreadPoolExecutor(5,
                                                                                                   new org.jboss.messaging.utils.JBMThreadFactory("jbm-pinger-threads"));

   private final Map<Object, ConnectionEntry> connections = new LinkedHashMap<Object, ConnectionEntry>();

   private int refCount;

   private boolean connected;

   private Iterator<ConnectionEntry> mapIterator;

   private Object failConnectionLock = new Object();

   private final long retryInterval;

   private final double retryIntervalMultiplier; // For exponential backoff

   private final int initialConnectAttempts;

   private final int reconnectAttempts;

   private volatile boolean closed;

   private boolean inFailoverOrReconnect;

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

   public ConnectionManagerImpl(final TransportConfiguration connectorConfig,
                                final TransportConfiguration backupConfig,
                                final int maxConnections,
                                final long callTimeout,
                                final long pingPeriod,
                                final long connectionTTL,
                                final long retryInterval,
                                final double retryIntervalMultiplier,
                                final int initialConnectAttempts,
                                final int reconnectAttempts)
   {
      this.connectorConfig = connectorConfig;

      this.backupConfig = backupConfig;

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

      this.pingPeriod = pingPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.initialConnectAttempts = initialConnectAttempts;

      this.reconnectAttempts = reconnectAttempts;
   }

   // ConnectionLifeCycleListener implementation --------------------

   public void connectionCreated(final Connection connection)
   {
   }

   public void connectionDestroyed(final Object connectionID)
   {
      MessagingException me = new MessagingException(MessagingException.OBJECT_CLOSED,
                                                     "The connection has been closed by the server");

      failoverOrReconnect(me, connectionID);
   }

   public void connectionException(final Object connectionID, final MessagingException me)
   {
      failoverOrReconnect(me, connectionID);
   }

   // ConnectionManager implementation ------------------------------------------------------------------

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final int ackBatchSize,
                                      final int minLargeMessageSize,
                                      final boolean blockOnAcknowledge,
                                      final boolean autoGroup,
                                      final int sendWindowSize,
                                      final int consumerWindowSize,
                                      final int consumerMaxRate,
                                      final int producerMaxRate,
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
                  connection = getConnectionForCreateSession();

                  if (connection == null)
                  {
                     // This can happen if the connection manager gets closed - e.g. the server gets shut down
                     return null;
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
                                                         sendWindowSize);

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

                  Channel sessionChannel = connection.getChannel(sessionChannelID, sendWindowSize, sendWindowSize != -1);

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
                                                                        minLargeMessageSize,
                                                                        connection,
                                                                        response.getServerVersion(),
                                                                        sessionChannel);

                  sessions.put(session, connection);

                  ChannelHandler handler = new ClientSessionPacketHandler(session, sessionChannel);

                  sessionChannel.setHandler(handler);

                  connected = true;

                  return session;
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
      throw new IllegalStateException("Oh my God it's full of stars");
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
            if (sessions.remove(session) == null)
            {
               throw new IllegalStateException("Cannot find session to remove " + session);
            }

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

   public void close()
   {
      closed = true;
   }

   private boolean handleConnectionFailed(final MessagingException me, final Object connectionID)
   {
      boolean callNext = !failoverOrReconnect(me, connectionID);

      return callNext;
   }

   // Public
   // ---------------------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Package Private
   // ------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private RemotingConnection getConnectionForCreateSession() throws MessagingException
   {
      int connectAttempts = connected ? 2 : initialConnectAttempts;
      
      int count = 0;

      if (connectAttempts != 0)
      {
         while (true)
         {
            if (closed)
            {
               return null;
            }

            RemotingConnection connection = getConnection(1);

            if (connection == null)
            {
               // We failed to get a connection

               // We now call failover() - this will attempt to reconnect/failover any pre-existing connections
               // If there are no pre-existing connections it will just return true, so we need to continue in a loop
               // here

               MessagingException me = new MessagingException(MessagingException.NOT_CONNECTED,
                                                              "Unabled to create session - server is unavailable and no backup server or backup is unavailable");

               boolean failedOver = failoverOrReconnect(me, null);

               if (!failedOver)
               {
                  // Nothing we can do here
                  throw me;
               }

               if (connectAttempts != -1)
               {
                  count++;

                  if (count == connectAttempts)
                  {
                     break;
                  }
               }

               try
               {
                  Thread.sleep(retryInterval);
               }
               catch (Exception ignore)
               {
               }
            }
            else
            {
               return connection;
            }
         }
      }

      throw new MessagingException(MessagingException.NOT_CONNECTED,
                                   "Unabled to create session after " + connectAttempts + " attempts");
   }

   private boolean failoverOrReconnect(final MessagingException me, final Object connectionID)
   {
      // To prevent recursion
      if (inFailoverOrReconnect)
      {
         return false;
      }
      
      synchronized (failoverLock)
      {
         if (connectionID != null && !connections.containsKey(connectionID))
         {
            // We already failed over - probably the first failure came in, all the connetions were failed over then a
            // async connection exception or disconnect
            // came in for one of the already closed connections, so we return true - we don't want to call the
            // listeners again

            return true;
         }

         inFailoverOrReconnect = true;

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

         boolean done = false;

         int connectAttempts;

         if (!connected)
         {
            connectAttempts = initialConnectAttempts;
         }
         else
         {
            connectAttempts = backupConnectorFactory == null ? reconnectAttempts : 0;
         }

         if (backupConnectorFactory != null || connectAttempts != 0)
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

            if (connectAttempts != 0)
            {
               // First try reconnecting to current node if configured to do this

               done = reattachSessions(connectAttempts);
            }
            else
            {
               // Now try failing over to backup

               connectorFactory = backupConnectorFactory;

               transportParams = backupTransportParams;

               backupConnectorFactory = null;

               backupTransportParams = null;

               done = reattachSessions(connectAttempts);
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
               // Fail the old connections so their listeners get called
               for (RemotingConnection connection : oldConnections)
               {
                  connection.fail(me);
               }
            }
         }
         else
         {
            // Just fail the connections

            failConnection(me);
         }

         inFailoverOrReconnect = false;

         return done;
      }
   }

   /*
    * Re-attach sessions all pre-existing sessions to new remoting connections
    */
   private boolean reattachSessions(final int connectAttempts)
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

         RemotingConnection backupConnection = getConnectionWithRetry(theSessions, connectAttempts);

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

   private RemotingConnection getConnectionWithRetry(final List<ClientSessionInternal> sessions,
                                                     final int connectAttempts)
   {
      long interval = retryInterval;

      int count = 0;

      while (true)
      {
         if (closed)
         {
            log.warn("ConnectionManager is now closed");

            return null;
         }

         RemotingConnection connection = getConnection(sessions.size());

         if (connection == null)
         {
            // Failed to get backup connection

            if (connectAttempts != 0)
            {
               count++;

               if (connectAttempts != -1 && count == connectAttempts)
               {
                  log.warn("Retried " + connectAttempts + " times to reconnect. Now giving up.");

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
            try
            {
               entry.connector.close();
            }
            catch (Throwable ignore)
            {
            }
         }

         mapIterator = null;
      }
   }

   private RemotingConnection getConnection(final int count)
   {
      RemotingConnection conn;

      if (connections.size() < maxConnections)
      {
         // Create a new one

         DelegatingBufferHandler handler = new DelegatingBufferHandler();

         Connector connector = null;

         Connection tc = null;

         try
         {
            connector = connectorFactory.createConnector(transportParams, handler, this);
            
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
                  {}
                  
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
               {}
            }
            
            if (connector != null)
            {
               try
               {
                  connector.close();
               }
               catch (Throwable t)
               {}
            }

            tc = null;

            connector = null;
         }
  
         if (tc == null)
         {
            return null;
         }

         conn = new RemotingConnectionImpl(tc, callTimeout, pingPeriod, connectionTTL, pingExecutor, null);

         conn.addFailureListener(new DelegatingFailureListener(conn.getID()));

         handler.conn = conn;

         conn.startPinger();

         connections.put(conn.getID(), new ConnectionEntry(conn, connector));

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

      refCount += count;

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
         // Can be legitimately null if session was closed before then went to remove session from csf
         // and locked since failover had started then after failover removes it but it's already been failed
      }
   }

   private void failConnection(final MessagingException me)
   {
      synchronized (failConnectionLock)
      {
         // When a single connection fails, we fail *all* the connections

         Set<ConnectionEntry> copy = new HashSet<ConnectionEntry>(connections.values());

         for (ConnectionEntry entry : copy)
         {
            entry.connection.fail(me);
         }

         refCount = 0;
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

   private class ConnectionEntry
   {
      ConnectionEntry(final RemotingConnection connection, final Connector connector)
      {
         this.connection = connection;

         this.connector = connector;
      }

      final RemotingConnection connection;

      final Connector connector;
   }

   private class DelegatingBufferHandler extends AbstractBufferHandler
   {
      RemotingConnection conn;

      public void bufferReceived(final Object connectionID, final MessagingBuffer buffer)
      {
         conn.bufferReceived(connectionID, buffer);
      }
   }

   private class DelegatingFailureListener implements FailureListener
   {
      final Object connectionID;

      DelegatingFailureListener(final Object connectionID)
      {
         this.connectionID = connectionID;
      }

      public boolean connectionFailed(final MessagingException me)
      {
         return handleConnectionFailed(me, connectionID);
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

}
