/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

import java.io.Serializable;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.Location;
import org.jboss.messaging.core.client.ConnectionParams;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
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
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
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

   // Attributes -----------------------------------------------------------------------------------
   
   private final Location location;

   private final ConnectionParams connectionParams;
 
   private final boolean strictTck;
      
   private int defaultConsumerWindowSize;
   
   private int defaultConsumerMaxRate;

   private int defaultProducerWindowSize;
   
   private int defaultProducerMaxRate;
   
   
   // Static ---------------------------------------------------------------------------------------
    
   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionFactoryImpl(final Location location, final ConnectionParams connectionParams,
                                      final boolean strictTck,
                                      final int defaultConsumerWindowSize, final int defaultConsumerMaxRate,
                                      final int defaultProducerWindowSize, final int defaultProducerMaxRate)
   {
      this.location = location;
      this.strictTck = strictTck;
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;  
      this.defaultConsumerMaxRate = defaultConsumerMaxRate;
      this.defaultProducerWindowSize = defaultProducerWindowSize;
      this.defaultProducerMaxRate = defaultProducerMaxRate;
      this.connectionParams = connectionParams;
   }
   
   public ClientConnectionFactoryImpl(final Location location)
   {
      this(location, new ConnectionParamsImpl(), false);
   }

   public ClientConnectionFactoryImpl(final Location location, final ConnectionParams connectionParams)
   {
      this(location, connectionParams, false);
   }
   
   protected ClientConnectionFactoryImpl(final Location location, final ConnectionParams connectionParams, final boolean dummy)
   {
      this.strictTck = false;
      this.defaultConsumerWindowSize = 1024 * 1024;      
      this.defaultConsumerMaxRate = -1;
      this.defaultProducerWindowSize = 1000;
      this.defaultProducerMaxRate = -1;
      this.location = location;
      this.connectionParams = connectionParams;
   }
   
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
         remotingConnection = new RemotingConnectionImpl(location, connectionParams);
       
         remotingConnection.start();
         
         long sessionID = remotingConnection.getSessionID();
         
         CreateConnectionRequest request =
            new CreateConnectionRequest(clientVersion.getIncrementingVersion(), sessionID, username, password);
         
         CreateConnectionResponse response =
            (CreateConnectionResponse)remotingConnection.sendBlocking(0, 0, request);

         return new ClientConnectionImpl(response.getConnectionTargetID(), strictTck, remotingConnection,
               defaultConsumerWindowSize, defaultConsumerMaxRate,
               defaultProducerWindowSize, defaultProducerMaxRate, response.getServerVersion());
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
   
   // ClientConnectionFactory implementation ---------------------------------------------

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

	public boolean isStrictTck()
	{
		return strictTck;
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
   
   public ConnectionParams getConnectionParams()
   {
      return connectionParams;
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
