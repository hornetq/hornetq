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

import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.ConnectionRegistryImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.ConnectorFactory;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.VersionLoader;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @version <tt>$Revision: 3602 $</tt>
 *
 */
public class ClientSessionFactoryImpl implements ClientSessionFactory
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;
   
   private static final Logger log = Logger.getLogger(ClientSessionFactoryImpl.class);
   
   public static final long DEFAULT_PING_PERIOD = 5000;
   
   public static final long DEFAULT_CALL_TIMEOUT = 30000;
   
   public static final int DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;
   
   public static final int DEFAULT_CONSUMER_MAX_RATE = -1;
   
   public static final int DEFAULT_PRODUCER_WINDOW_SIZE = 1024 * 1024;
   
   public static final int DEFAULT_PRODUCER_MAX_RATE = -1;
   
   public static final boolean DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;
   
   public static final boolean DEFAULT_BLOCK_ON_PERSISTENT_SEND = false;
   
   public static final boolean DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND = false;

   // Attributes -----------------------------------------------------------------------------------
     
   private ConnectionRegistry connectionRegistry;
   
   //These attributes are mutable and can be updated by different threads so must be volatile

   private volatile ConnectorFactory connectorFactory;
   
   private volatile Map<String, Object> transportParams;
   
   private volatile long pingPeriod;
   
   private volatile long callTimeout;
   
   private volatile int consumerWindowSize;
   
   private volatile int consumerMaxRate;

   private volatile int producerWindowSize;
   
   private volatile int producerMaxRate;
   
   private volatile boolean blockOnAcknowledge;
   
   private volatile boolean blockOnPersistentSend;
   
   private volatile boolean blockOnNonPersistentSend;
        
   // Static ---------------------------------------------------------------------------------------
   
   // Constructors ---------------------------------------------------------------------------------

   /**
    * Create a ClientSessionFactoryImpl specifying all attributes
    */
   public ClientSessionFactoryImpl(final ConnectorFactory connectorFactory,
                                   final Map<String, Object> transportParams,
                                   final long pingPeriod,
                                   final long callTimeout,
                                   final int consumerWindowSize, final int consumerMaxRate,
                                   final int producerWindowSize, final int producerMaxRate,
                                   final boolean blockOnAcknowledge,
                                   final boolean blockOnNonPersistentSend,
                                   final boolean blockOnPersistentSend)
   {      
      this.connectorFactory = connectorFactory;
      this.transportParams = transportParams;
      this.pingPeriod = pingPeriod;
      this.callTimeout = callTimeout;
      this.consumerWindowSize = consumerWindowSize;  
      this.consumerMaxRate = consumerMaxRate;
      this.producerWindowSize = producerWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.connectionRegistry = ConnectionRegistryImpl.instance;
   }
   
   /**
    * Create a ClientSessionFactoryImpl specify transport type and using defaults
    */   
   public ClientSessionFactoryImpl(final ConnectorFactory connectorFactory)
   {
      this.connectorFactory = connectorFactory;
      this.transportParams = new HashMap<String, Object>();
      pingPeriod = DEFAULT_PING_PERIOD;
      callTimeout = DEFAULT_CALL_TIMEOUT;
      consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;
      consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;
      producerWindowSize = DEFAULT_PRODUCER_WINDOW_SIZE;
      producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;
      blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;      
      this.connectionRegistry = ConnectionRegistryImpl.instance;
   }
         
   // ClientSessionFactory implementation ---------------------------------------------

   public ClientSession createSession(final String username, final String password, final boolean xa,
                                      final boolean autoCommitSends, final boolean autoCommitAcks,
                                      int lazyAckBatchSize, boolean cacheProducers)                 
      throws MessagingException
   {
      return createSessionInternal(username, password, xa, autoCommitSends, autoCommitAcks, lazyAckBatchSize,
                                   cacheProducers);
   }
   
   public ClientSession createSession(final boolean xa,
            final boolean autoCommitSends, final boolean autoCommitAcks,
            int lazyAckBatchSize, boolean cacheProducers)                 
      throws MessagingException
   {
      return createSessionInternal(null, null, xa, autoCommitSends, autoCommitAcks, lazyAckBatchSize,
               cacheProducers);
   }   
   
	public int getConsumerWindowSize()
	{
		return consumerWindowSize;
	}
	
	public void setConsumerWindowSize(final int size)
   {
      consumerWindowSize = size;
   }
	
	public int getProducerWindowSize()
	{
		return producerWindowSize;
	}
				
	public void setProducerWindowSize(final int size)
   {
      producerWindowSize = size;
   }
	
	public int getProducerMaxRate()
	{
		return producerMaxRate;
	}
		
	public void setProducerMaxRate(final int rate)
	{
	   this.producerMaxRate = rate;
	}
		
	public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }
   	
   public void setConsumerMaxRate(final int rate)
   {
      this.consumerMaxRate = rate;
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
      return this.blockOnAcknowledge;
   }
   
   public void setBlockOnAcknowledge(final boolean blocking)
   {
      blockOnAcknowledge = blocking;
   }
   
   public ConnectorFactory getConnectorFactory()
   {
      return connectorFactory;
   }

   public void setConnectorFactory(final ConnectorFactory connectorFactory)
   {
      this.connectorFactory = connectorFactory;
   }

   public Map<String, Object> getTransportParams()
   {
      return transportParams;
   }

   public void setTransportParams(final Map<String, Object> transportParams)
   {
      this.transportParams = transportParams;
   }

   public long getPingPeriod()
   {
      return pingPeriod;
   }

   public void setPingPeriod(final long pingPeriod)
   {
      this.pingPeriod = pingPeriod;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public void setCallTimeout(final long callTimeout)
   {
      this.callTimeout = callTimeout;
   }
            
   // Public ---------------------------------------------------------------------------------------
   
   public void setConnectionRegistry(final ConnectionRegistry registry)
   {
      this.connectionRegistry = registry;
   }
           
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private ClientSession createSessionInternal(final String username, final String password, final boolean xa,
            final boolean autoCommitSends, final boolean autoCommitAcks,
            int lazyAckBatchSize, boolean cacheProducers)                 
      throws MessagingException
   {
      Version clientVersion = VersionLoader.load();

      RemotingConnection remotingConnection = null;
      try
      {
         remotingConnection = connectionRegistry.getConnection(connectorFactory, transportParams,
                                                               pingPeriod, callTimeout);

         Packet request =
            new CreateSessionMessage(clientVersion.getIncrementingVersion(),
                                     username, password,
                                     xa, autoCommitSends, autoCommitAcks);
         
         Channel channel1 = remotingConnection.getChannel(1, false, -1);
         
         Packet packet = channel1.sendBlocking(request);
         
         if (packet.getType() == PacketImpl.EXCEPTION)
         {
            MessagingExceptionMessage mem = (MessagingExceptionMessage)packet;
            
            throw mem.getException();
         }
                           
         CreateSessionResponseMessage response = (CreateSessionResponseMessage)packet;
         
         long sessionID = response.getSessionID();
         
         Channel sessionChannel = remotingConnection.getChannel(sessionID, false, response.getPacketConfirmationBatchSize());
         
         ClientSessionInternal session = new ClientSessionImpl(sessionID, xa, lazyAckBatchSize, cacheProducers,
                  autoCommitSends, autoCommitAcks, blockOnAcknowledge,
                  remotingConnection, this,                  
                  response.getServerVersion(), sessionChannel);
         
         ChannelHandler handler = new ClientSessionPacketHandler(session);
         
         sessionChannel.setHandler(handler);
         
         return session;
                  
      }
      catch (Throwable t)
      {
         if (remotingConnection != null)
         {
            try
            {
               connectionRegistry.returnConnection(remotingConnection.getID());  
            }
            catch (Throwable ignore)
            {               
            }
         }

         if (t instanceof MessagingException)
         {
            throw (MessagingException)t;
         }
         else
         {
            MessagingException me = new MessagingException(MessagingException.INTERNAL_ERROR, "Failed to start connection");

            me.initCause(t);

            throw me;
         }
      }
   }

   
   // Inner Classes --------------------------------------------------------------------------------
}
