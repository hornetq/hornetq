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

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ConnectionRegistry;
import org.jboss.messaging.core.remoting.ConnectionRegistryLocator;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionResponseMessage;
import org.jboss.messaging.core.server.CommandManager;
import org.jboss.messaging.core.server.impl.CommandManagerImpl;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.UUIDGenerator;
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
   
   public static final int DEFAULT_DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;
   
   public static final int DEFAULT_DEFAULT_CONSUMER_MAX_RATE = -1;
   
   public static final int DEFAULT_DEFAULT_PRODUCER_WINDOW_SIZE = 1024 * 1024;
   
   public static final int DEFAULT_DEFAULT_PRODUCER_MAX_RATE = -1;
   
   public static final boolean DEFAULT_DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;
   
   public static final boolean DEFAULT_DEFAULT_BLOCK_ON_PERSISTENT_SEND = false;
   
   public static final boolean DEFAULT_DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND = false;

   // Attributes -----------------------------------------------------------------------------------
     
   private final Location location;
   
   private ConnectionRegistry connectionRegistry;
   
   //These attributes are mutable and can be updated by different threads so must be volatile

   private volatile ConnectionParams connectionParams;
 
   private volatile int defaultConsumerWindowSize;
   
   private volatile int defaultConsumerMaxRate;

   private volatile int defaultProducerWindowSize;
   
   private volatile int defaultProducerMaxRate;
   
   private volatile boolean defaultBlockOnAcknowledge;
   
   private volatile boolean defaultBlockOnPersistentSend;
   
   private volatile boolean defaultBlockOnNonPersistentSend;
        
   // Static ---------------------------------------------------------------------------------------
   
   // Constructors ---------------------------------------------------------------------------------

   /**
    * Create a ClientSessionFactoryImpl specifying all attributes
    */
   public ClientSessionFactoryImpl(final Location location, final ConnectionParams connectionParams,
                                      final int defaultConsumerWindowSize, final int defaultConsumerMaxRate,
                                      final int defaultProducerWindowSize, final int defaultProducerMaxRate,
                                      final boolean defaultBlockOnAcknowledge,
                                      final boolean defaultSendNonPersistentMessagesBlocking,
                                      final boolean defaultSendPersistentMessagesBlocking)
   {      
      this.location = location;
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;  
      this.defaultConsumerMaxRate = defaultConsumerMaxRate;
      this.defaultProducerWindowSize = defaultProducerWindowSize;
      this.defaultProducerMaxRate = defaultProducerMaxRate;
      this.defaultBlockOnAcknowledge = defaultBlockOnAcknowledge;
      this.defaultBlockOnNonPersistentSend = defaultSendNonPersistentMessagesBlocking;
      this.defaultBlockOnPersistentSend = defaultSendPersistentMessagesBlocking;
      this.connectionParams = connectionParams;
      this.connectionRegistry = ConnectionRegistryLocator.getRegistry();
   }
   
   /**
    * Create a ClientSessionFactoryImpl specify location and using all default attribute values
    * @param location the location of the server
    */
   public ClientSessionFactoryImpl(final Location location)
   {
      this(location, new ConnectionParamsImpl(), false);   
   }

   /**
    * Create a ClientSessionFactoryImpl specify location and connection params and using all other default attribute values
    * @param location the location of the server
    * @param connectionParams the connection parameters
    */
   public ClientSessionFactoryImpl(final Location location, final ConnectionParams connectionParams)
   {
      this(location, connectionParams, false);
   }
   
   
   private ClientSessionFactoryImpl(final Location location, final ConnectionParams connectionParams,
                                    final boolean dummy)
   {
      defaultConsumerWindowSize = DEFAULT_DEFAULT_CONSUMER_WINDOW_SIZE;
      defaultConsumerMaxRate = DEFAULT_DEFAULT_CONSUMER_MAX_RATE;
      defaultProducerWindowSize = DEFAULT_DEFAULT_PRODUCER_WINDOW_SIZE;
      defaultProducerMaxRate = DEFAULT_DEFAULT_PRODUCER_MAX_RATE;
      defaultBlockOnAcknowledge = DEFAULT_DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      defaultBlockOnPersistentSend = DEFAULT_DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      defaultBlockOnNonPersistentSend = DEFAULT_DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;      
      this.location = location;
      this.connectionParams = connectionParams;
      this.connectionRegistry = ConnectionRegistryLocator.getRegistry();
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
   
	public int getDefaultConsumerWindowSize()
	{
		return defaultConsumerWindowSize;
	}
	
	public void setDefaultConsumerWindowSize(final int size)
   {
      defaultConsumerWindowSize = size;
   }
	
	public int getDefaultProducerWindowSize()
	{
		return defaultProducerWindowSize;
	}
				
	public void setDefaultProducerWindowSize(final int size)
   {
      defaultProducerWindowSize = size;
   }
	
	public int getDefaultProducerMaxRate()
	{
		return defaultProducerMaxRate;
	}
		
	public void setDefaultProducerMaxRate(final int rate)
	{
	   this.defaultProducerMaxRate = rate;
	}
		
	public int getDefaultConsumerMaxRate()
   {
      return defaultConsumerMaxRate;
   }
   	
   public void setDefaultConsumerMaxRate(final int rate)
   {
      this.defaultConsumerMaxRate = rate;
   }
   
   public boolean isDefaultBlockOnPersistentSend()
   {
      return defaultBlockOnPersistentSend;
   }
   
   public void setDefaultBlockOnPersistentSend(final boolean blocking)
   {
      defaultBlockOnPersistentSend = blocking;
   }
   
   public boolean isDefaultBlockOnNonPersistentSend()
   {
      return defaultBlockOnNonPersistentSend;
   }
   
   public void setDefaultBlockOnNonPersistentSend(final boolean blocking)
   {
      defaultBlockOnNonPersistentSend = blocking;
   }
   
   public boolean isDefaultBlockOnAcknowledge()
   {
      return this.defaultBlockOnAcknowledge;
   }
   
   public void setDefaultBlockOnAcknowledge(final boolean blocking)
   {
      defaultBlockOnAcknowledge = blocking;
   }
            
   public ConnectionParams getConnectionParams()
   {
      return connectionParams;
   }
    
   public void setConnectionParams(final ConnectionParams params)
   {
      this.connectionParams = params;
   }
   
   public Location getLocation()
   {
      return location;
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
         remotingConnection = connectionRegistry.getConnection(location, connectionParams);

         PacketDispatcher dispatcher = remotingConnection.getPacketDispatcher();

         long localCommandResponseTargetID = dispatcher.generateID();

         String name = UUIDGenerator.getInstance().generateSimpleStringUUID().toString();
         
         Packet request =
            new CreateSessionMessage(name, clientVersion.getIncrementingVersion(),
                                     username, password,
                                     xa, autoCommitSends, autoCommitAcks,
                                     localCommandResponseTargetID);

         CreateSessionResponseMessage response =
            (CreateSessionResponseMessage)remotingConnection.sendBlocking(PacketDispatcherImpl.MAIN_SERVER_HANDLER_ID,
                     PacketDispatcherImpl.MAIN_SERVER_HANDLER_ID, request, null);
         
         CommandManager cm = new CommandManagerImpl(connectionParams.getPacketConfirmationBatchSize(),
                  remotingConnection, dispatcher, response.getSessionID(),
                  localCommandResponseTargetID, response.getCommandResponseTargetID());
   
         return new ClientSessionImpl(name, response.getSessionID(), xa, lazyAckBatchSize, cacheProducers,
                  autoCommitSends, autoCommitAcks, defaultBlockOnAcknowledge,
                  remotingConnection, this,
                  dispatcher,
                  response.getServerVersion(), cm);
      }
      catch (Throwable t)
      {
         if (remotingConnection != null)
         {
            try
            {
               connectionRegistry.returnConnection(location);  
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
