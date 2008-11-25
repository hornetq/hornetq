/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.EARLY_RESPONSE;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.ConnectionManager;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.ConnectionManagerImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.UUIDGenerator;
import org.jboss.messaging.util.VersionLoader;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 3602 $</tt>
 * 
 * Note! There should never be more than one clientsessionfactory with the same connection params
 * Otherwise failover won't work properly since channel ids won't match on live and backup
 */
public class ClientSessionFactoryImpl implements ClientSessionFactoryInternal, FailureListener
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(ClientSessionFactoryImpl.class);

   public static final long DEFAULT_PING_PERIOD = 5000;

   // Any message beyond this size is considered a big message (to be chunked)
   public static final int DEFAULT_BIG_MESSAGE_SIZE = 100 * 1024;

   public static final int DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_CONSUMER_MAX_RATE = -1;

   public static final int DEFAULT_SEND_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_PRODUCER_MAX_RATE = -1;

   public static final boolean DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;

   public static final boolean DEFAULT_BLOCK_ON_PERSISTENT_SEND = false;

   public static final boolean DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND = false;

   public static final boolean DEFAULT_AUTO_GROUP = false;
   
   public static final long DEFAULT_CALL_TIMEOUT = 30000;
      
   public static final int DEFAULT_MAX_CONNECTIONS = 8;
   
   public static final int DEFAULT_ACK_BATCH_SIZE = 1024 * 1024;

   public static final boolean DEFAULT_PRE_ACKNOWLEDGE = false;

   // Attributes
   // -----------------------------------------------------------------------------------

   // These attributes are mutable and can be updated by different threads so
   // must be volatile

   private final ConnectorFactory connectorFactory;

   private final Map<String, Object> transportParams;

   private final ConnectorFactory backupConnectorFactory;

   private final Map<String, Object> backupTransportParams;

   private final long pingPeriod;

   private final long callTimeout;

   private final int maxConnections;

   private volatile ConnectionManager connectionManager;

   private volatile ConnectionManager backupConnectionManager;
   
   private volatile int minLargeMessageSize;

   private volatile int consumerWindowSize;

   private volatile int consumerMaxRate;

   private volatile int sendWindowSize;

   private volatile int producerMaxRate;

   private volatile boolean blockOnAcknowledge;

   private volatile boolean blockOnPersistentSend;

   private volatile boolean blockOnNonPersistentSend;

   private volatile boolean autoGroup;

   private boolean preAcknowledge;
   
   private volatile int ackBatchSize;

   private final Set<ClientSessionInternal> sessions = new HashSet<ClientSessionInternal>();
   
   private final Object exitLock = new Object();
   
   private final Object createSessionLock = new Object();
   
   private boolean inCreateSession;
   
   private final Object failoverLock = new Object();
   

   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   /**
    * Create a ClientSessionFactoryImpl specifying all attributes
    */
   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConfig,
                                   final long pingPeriod,                                 
                                   final long callTimeout,
                                   final int consumerWindowSize,
                                   final int consumerMaxRate,
                                   final int sendWindowSize,
                                   final int producerMaxRate,
                                   final int minLargeMessageSize,
                                   final boolean blockOnAcknowledge,
                                   final boolean blockOnNonPersistentSend,
                                   final boolean blockOnPersistentSend,
                                   final boolean autoGroup,
                                   final int maxConnections,
                                   final boolean preAcknowledge,
                                   final int ackBatchSize)
   {
      connectorFactory = instantiateConnectorFactory(connectorConfig.getFactoryClassName());

      transportParams = connectorConfig.getParams();

      connectionManager = new ConnectionManagerImpl(connectorFactory,
                                                    transportParams,
                                                    pingPeriod,
                                                    callTimeout,
                                                    maxConnections);
      if (backupConfig != null)
      {
         backupConnectorFactory = instantiateConnectorFactory(backupConfig.getFactoryClassName());

         backupTransportParams = backupConfig.getParams();

         backupConnectionManager = new ConnectionManagerImpl(backupConnectorFactory,
                                                             backupTransportParams,
                                                             pingPeriod,
                                                             callTimeout,
                                                             maxConnections);
      }
      else
      {
         backupConnectorFactory = null;

         backupTransportParams = null;

         backupConnectionManager = null;
      }
      this.pingPeriod = pingPeriod;
      this.callTimeout = callTimeout;
      this.consumerWindowSize = consumerWindowSize;
      this.consumerMaxRate = consumerMaxRate;
      this.sendWindowSize = sendWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.minLargeMessageSize = minLargeMessageSize;
      this.autoGroup = autoGroup;
      this.maxConnections = maxConnections;
      this.ackBatchSize = ackBatchSize;
      this.preAcknowledge = preAcknowledge;
   }

   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConfig)
   {
      this(connectorConfig,
           backupConfig,
           DEFAULT_PING_PERIOD,
           DEFAULT_CALL_TIMEOUT,
           DEFAULT_CONSUMER_WINDOW_SIZE,
           DEFAULT_CONSUMER_MAX_RATE,
           DEFAULT_SEND_WINDOW_SIZE,
           DEFAULT_PRODUCER_MAX_RATE,
           DEFAULT_BIG_MESSAGE_SIZE,
           DEFAULT_BLOCK_ON_ACKNOWLEDGE,
           DEFAULT_BLOCK_ON_PERSISTENT_SEND,
           DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
           DEFAULT_AUTO_GROUP,
           DEFAULT_MAX_CONNECTIONS,
           DEFAULT_PRE_ACKNOWLEDGE,
           DEFAULT_ACK_BATCH_SIZE);
   }

   /**
    * Create a ClientSessionFactoryImpl specify transport type and using defaults
    */
   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig)
   {
      this(connectorConfig, null);
   }

   // ClientSessionFactory implementation
   // ---------------------------------------------

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final int ackBatchSize) throws MessagingException
   {
      return createSessionInternal(username, password, xa, autoCommitSends, autoCommitAcks, preAcknowledge, ackBatchSize);
   }

   public ClientSession createSession(final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks) throws MessagingException
   {
      return createSessionInternal(null, null, xa, autoCommitSends, autoCommitAcks, preAcknowledge, ackBatchSize);
   }

   public ClientSession createSession(final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge) throws MessagingException
   {
      return createSessionInternal(null, null, xa, autoCommitSends, autoCommitAcks, preAcknowledge, ackBatchSize);
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final int size)
   {
      consumerWindowSize = size;
   }

   public int getSendWindowSize()
   {
      return sendWindowSize;
   }

   public void setSendWindowSize(final int size)
   {
      sendWindowSize = size;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public void setProducerMaxRate(final int rate)
   {
      producerMaxRate = rate;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final int rate)
   {
      consumerMaxRate = rate;
   }

   public boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }

   public void setBlockOnPersistentSend(final boolean blocking)
   {
      blockOnPersistentSend = blocking;
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }

   public void setBlockOnNonPersistentSend(final boolean blocking)
   {
      blockOnNonPersistentSend = blocking;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final boolean blocking)
   {
      blockOnAcknowledge = blocking;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public void setAutoGroup(boolean autoGroup)
   {
      this.autoGroup = autoGroup;
   }
   
   public int getAckBatchSize()
   {
      return ackBatchSize;
   }
   
   public void setAckBatchSize(int ackBatchSize)
   {
      this.ackBatchSize = ackBatchSize;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public void setPreAcknowledge(boolean preAcknowledge)
   {
      this.preAcknowledge = preAcknowledge;
   }

   public ConnectorFactory getConnectorFactory()
   {
      return connectorFactory;
   }

   public Map<String, Object> getTransportParams()
   {
      return transportParams;
   }

   public ConnectorFactory getBackupConnectorFactory()
   {
      return backupConnectorFactory;
   }

   public Map<String, Object> getBackupTransportParams()
   {
      return backupTransportParams;
   }

   public long getPingPeriod()
   {
      return pingPeriod;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public int getMaxConnections()
   {
      return maxConnections;
   }

   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   /**
    * @param minLargeMessageSize the minLargeMessageSize to set
    */
   public void setMinLargeMessageSize(int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize;
   }

   // ClientSessionFactoryInternal implementation
   // ------------------------------------------

   // Must be synchronized to prevent it happening concurrently with failover which can lead to
   // inconsistencies
   public void removeSession(final ClientSessionInternal session)
   {
      //TODO - can we simplify this locking?
      synchronized (createSessionLock)
      {
         synchronized (failoverLock)
         {
            if (!sessions.remove(session))
            {
               throw new IllegalStateException("Cannot find session to remove " + session);
            }
      
            connectionManager.returnConnection(session.getConnection().getID());
      
            if (backupConnectionManager != null)
            {
               backupConnectionManager.returnConnection(session.getBackupConnection().getID());
            }
         }
      }
   }

   public int numConnections()
   {
      return connectionManager.numConnections();
   }

   public int numBackupConnections()
   {
      return backupConnectionManager != null ? backupConnectionManager.numConnections() : 0;
   }

   public int numSessions()
   {
      return sessions.size();
   }

   // FailureListener implementation --------------------------------------------------------

   public void connectionFailed(final MessagingException me)
   {      
      if (me.getCode() == MessagingException.OBJECT_CLOSED)
      {
         //The server has closed the connection. We don't want failover to occur in this case - 
         //either the server has booted off the connection, or it didn't receive a ping in time
         //in either case server side resources on both live and backup will be removed so the client
         //can't failover anyway
         return;
      }
      
      synchronized (failoverLock)
      {         
         //Now get locks on all channel 1s, whilst holding the failoverLock - this makes sure
         //There are either no threads executing in createSession, or one is blocking on a createSession
         //result.
         
         //Then interrupt the channel 1 that is blocking (could just interrupt them all)
         
         //Then release all channel 1 locks - this allows the createSession to exit the monitor
         
         //Then get all channel 1 locks again - this ensures the any createSession thread has executed the section and
         //returned all its connections to the connection manager (the code to return connections to connection manager
         //must be inside the lock
         
         //Then perform failover
         
         //Then release failoverLock
         
         //The other side of the bargain - during createSession:
         //The calling thread must get the failoverLock and get its' connections when this is locked.
         //While this is still locked it must then get the channel1 lock
         //It can then release the failoverLock
         //It should catch MessagingException.INTERRUPTED in the call to channel.sendBlocking
         //It should then return its connections, with channel 1 lock still held
         //It can then release the channel 1 lock, and retry (which will cause locking on failoverLock
         //until failover is complete
         
         if (backupConnectionManager != null)
         {
            log.info("Commencing automatic failover");
            lockAllChannel1s();
            
            final boolean needToInterrupt;
            
            synchronized (exitLock)
            {
               needToInterrupt = inCreateSession;
            }
            
            unlockAllChannel1s();
                 
            if (needToInterrupt)
            {           
               //Forcing return all channels won't guarantee that any blocked thread will return immediately
               //So we need to wait for it
               forceReturnAllChannel1s();
               
               //Now we need to make sure that the thread has actually exited and returned it's connections
               //before failover occurs
               
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
            
            //Now we absolutely know that no threads are executing in or blocked in createSession, and no
            //more will execute it until failover is complete
            
            //So.. do failover
            
            connectionManager = backupConnectionManager;
   
            backupConnectionManager = null;
            
            for (ClientSessionInternal session : sessions)
            {
               session.handleFailover();
            }
            
            log.info("Failover complete");
         }                    
      }      
   }
   
   // Public
   // ---------------------------------------------------------------------------------------

   // Protected
   // ------------------------------------------------------------------------------------

   // Package Private
   // ------------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   
   //The whole method must be synchonrized to prevent more than one thread executing concurrently
   private ClientSession createSessionInternal(final String username,
                                               final String password,
                                               final boolean xa,
                                               final boolean autoCommitSends,
                                               final boolean autoCommitAcks,
                                               final boolean preAcknowledge,
                                               final int ackBatchSize) throws MessagingException
   {
      synchronized (createSessionLock)
      {
         String name = UUIDGenerator.getInstance().generateSimpleStringUUID().toString();
         boolean retry = false;
         do
         {         
            Version clientVersion = VersionLoader.getVersion();
      
            RemotingConnection connection = null;
      
            RemotingConnection backupConnection = null;
            
            Lock lock = null;
      
            try
            {    
               Channel channel1;
               
               synchronized (failoverLock)
               {               
                  connection = connectionManager.getConnection();
      
                  if (backupConnectionManager != null)
                  {
                     backupConnection = backupConnectionManager.getConnection();
                  }
                  
                  channel1 = connection.getChannel(1, -1, false);
                  
                  //Lock it - this must be done while the failoverLock is held
                  channel1.getLock().lock();
                  
                  lock = channel1.getLock();
               } //We can now release the failoverLock
               
               //We now set a flag saying createSession is executing
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
                  //This means the thread was blocked on create session and failover unblocked it
                  //so failover could occur
                  
                  //So we just need to return our connections and flag for retry
                  
                  connectionManager.returnConnection(connection.getID());               
         
                  backupConnectionManager.returnConnection(backupConnection.getID());
                  
                  retry = true;                             
               }
               else
               {         
                  CreateSessionResponseMessage response = (CreateSessionResponseMessage)pResponse;
         
                  Channel sessionChannel = connection.getChannel(sessionChannelID,
                                                                 sendWindowSize,
                                                                 sendWindowSize != -1);
         
                  ClientSessionInternal session = new ClientSessionImpl(this,
                                                                        name,
                                                                        xa,                                                                      
                                                                        autoCommitSends,
                                                                        autoCommitAcks,
                                                                        preAcknowledge,
                                                                        blockOnAcknowledge,
                                                                        autoGroup,
                                                                        ackBatchSize,
                                                                        connection,
                                                                        backupConnection,                                                                       
                                                                        response.getServerVersion(),
                                                                        sessionChannel);
                           
                  sessions.add(session);

                  ChannelHandler handler = new ClientSessionPacketHandler(session, sessionChannel);
         
                  sessionChannel.setHandler(handler);
         
                  connection.addFailureListener(this);
         
                  return session;
               }
            }
            catch (Throwable t)
            {
               if (connection != null)
               {
                  connectionManager.returnConnection(connection.getID());
               }
      
               if (backupConnection != null)
               {
                  backupConnectionManager.returnConnection(backupConnection.getID());
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
               
               //Execution has finished so notify any failover thread that may be waiting for us to be done
               synchronized (exitLock)
               {
                  inCreateSession = false;
                  
                  exitLock.notify();
               }                     
            }
         }
         while (retry);
      }
      
      //Should never get here
      throw new IllegalStateException("How did you get here??");
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
      Set<RemotingConnection> conns = connectionManager.getConnections();
      
      for (RemotingConnection conn: conns)
      {
         Channel channel1 = conn.getChannel(1, -1, false);
         
         channel1.getLock().lock();
      }
   }
   
   private void unlockAllChannel1s()
   {
      Set<RemotingConnection> conns = connectionManager.getConnections();
      
      for (RemotingConnection conn: conns)
      {
         Channel channel1 = conn.getChannel(1, -1, false);
         
         channel1.getLock().unlock();
      }
   }
   
   private void forceReturnAllChannel1s()
   {
      Set<RemotingConnection> conns = connectionManager.getConnections();
      
      for (RemotingConnection conn: conns)
      {
         Channel channel1 = conn.getChannel(1, -1, false);
         
         channel1.returnBlocking();
      }
   }
   

}
