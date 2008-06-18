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

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingConnectionFactory;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionFactoryImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.VersionLoader;


/**
 * Core connection factory.
 * 
 * Can be instantiate programmatically and used to make connections.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 *
 * @version <tt>$Revision: 3602 $</tt>
 *
 * $Id: ClientConnectionFactoryImpl.java 3602 2008-01-21 17:48:32Z timfox $
 */
public class ClientConnectionFactoryImpl implements ClientConnectionFactory
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;
   
   private static final Logger log = Logger.getLogger(ClientConnectionFactoryImpl.class);
   
   public static final int DEFAULT_DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;
   
   public static final int DEFAULT_DEFAULT_CONSUMER_MAX_RATE = -1;
   
   public static final int DEFAULT_DEFAULT_PRODUCER_WINDOW_SIZE = 1024 * 1024;
   
   public static final int DEFAULT_DEFAULT_PRODUCER_MAX_RATE = -1;
   
   public static final boolean DEFAULT_DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;
   
   public static final boolean DEFAULT_DEFAULT_BLOCK_ON_PERSISTENT_SEND = false;
   
   public static final boolean DEFAULT_DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND = false;

   // Attributes -----------------------------------------------------------------------------------
   
   private final RemotingConnectionFactory remotingConnectionFactory;
      
   private final Location location;
   
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
    * Create a ClientConnectionFactoryImpl specifying all attributes
    */
   public ClientConnectionFactoryImpl(final RemotingConnectionFactory remotingConnectionFactory,
                                      final Location location, final ConnectionParams connectionParams,
                                      final int defaultConsumerWindowSize, final int defaultConsumerMaxRate,
                                      final int defaultProducerWindowSize, final int defaultProducerMaxRate,
                                      final boolean defaultBlockOnAcknowledge,
                                      final boolean defaultSendNonPersistentMessagesBlocking,
                                      final boolean defaultSendPersistentMessagesBlocking)
   {
      this.remotingConnectionFactory = remotingConnectionFactory;
      this.location = location;
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;  
      this.defaultConsumerMaxRate = defaultConsumerMaxRate;
      this.defaultProducerWindowSize = defaultProducerWindowSize;
      this.defaultProducerMaxRate = defaultProducerMaxRate;
      this.defaultBlockOnAcknowledge = defaultBlockOnAcknowledge;
      this.defaultBlockOnNonPersistentSend = defaultSendNonPersistentMessagesBlocking;
      this.defaultBlockOnPersistentSend = defaultSendPersistentMessagesBlocking;
      this.connectionParams = connectionParams;
   }
   
   /**
    * Create a ClientConnectionFactoryImpl specify location and using all default attribute values
    * @param location the location of the server
    */
   public ClientConnectionFactoryImpl(final Location location)
   {
      this(location, new ConnectionParamsImpl(), false);   
   }

   /**
    * Create a ClientConnectionFactoryImpl specify location and connection params and using all other default attribute values
    * @param location the location of the server
    * @param connectionParams the connection parameters
    */
   public ClientConnectionFactoryImpl(final Location location, final ConnectionParams connectionParams)
   {
      this(location, connectionParams, false);
   }
   
   
   private ClientConnectionFactoryImpl(final Location location, final ConnectionParams connectionParams,
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
      this.remotingConnectionFactory = new RemotingConnectionFactoryImpl();
   }
         
   // ClientConnectionFactory implementation ---------------------------------------------

   public ClientConnection createConnection() throws MessagingException
   {
      return createConnection(null, null);
   }
   
   public ClientConnection createConnection(final String username, final String password) throws MessagingException
   {
      Version clientVersion = VersionLoader.load();
                       
      RemotingConnection remotingConnection = null;
      try
      {
         remotingConnection = remotingConnectionFactory.createRemotingConnection(location, connectionParams);
       
         remotingConnection.start();
         
         long sessionID = remotingConnection.getSessionID();
         
         CreateConnectionRequest request =
            new CreateConnectionRequest(clientVersion.getIncrementingVersion(), sessionID, username, password);
         
         CreateConnectionResponse response =
            (CreateConnectionResponse)remotingConnection.sendBlocking(0, 0, request);

         return new ClientConnectionImpl(this, response.getConnectionTargetID(), remotingConnection, response.getServerVersion());
      }
      catch (Throwable t)
      {
         if (remotingConnection != null)
         {
            try
            {
               remotingConnection.stop();
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
      
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   // Inner Classes --------------------------------------------------------------------------------
}
