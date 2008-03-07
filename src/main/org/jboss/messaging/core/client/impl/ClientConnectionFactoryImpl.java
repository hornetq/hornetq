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
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.impl.PacketDispatcherImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.version.Version;


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
public class ClientConnectionFactoryImpl implements ClientConnectionFactory, Serializable
{
   // Constants ------------------------------------------------------------------------------------
   public  static final String id = "CONNECTION_FACTORY_ID";

   private static final long serialVersionUID = 2512460695662741413L;
   
   private static final Logger log = Logger.getLogger(ClientConnectionFactoryImpl.class);

   // Attributes -----------------------------------------------------------------------------------
   
   private final RemotingConfiguration remotingConfig;
   
   private final PacketDispatcher dispatcher;

   private final Version serverVersion;
 
   private final int serverID;
   
   private final boolean strictTck;
      
   private final int defaultConsumerWindowSize;
   
   private final int defaultConsumerMaxRate;

   private final int defaultProducerWindowSize;
   
   private final int defaultProducerMaxRate;
   
   
   // Static ---------------------------------------------------------------------------------------
    
   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionFactoryImpl(final int serverID, final RemotingConfiguration remotingConfig,
   		                             final Version serverVersion, final boolean strictTck,
                                      final int defaultConsumerWindowSize, final int defaultConsumerMaxRate,
                                      final int defaultProducerWindowSize, final int defaultProducerMaxRate)
   {
      this.serverID = serverID;
      this.remotingConfig = remotingConfig;
      this.serverVersion = serverVersion;
      this.strictTck = strictTck;
      this.defaultConsumerWindowSize = defaultConsumerWindowSize;  
      this.defaultConsumerMaxRate = defaultConsumerMaxRate;
      this.defaultProducerWindowSize = defaultProducerWindowSize;
      this.defaultProducerMaxRate = defaultProducerMaxRate;
      this.dispatcher = new PacketDispatcherImpl();
   }
   
   public ClientConnectionFactoryImpl(final int serverID, final RemotingConfiguration remotingConfig,
                                      final Version serverVersion)
   {
      this.serverID = serverID;
      this.remotingConfig = remotingConfig;
      this.serverVersion = serverVersion;
      this.strictTck = false;
      this.defaultConsumerWindowSize = 1000;      
      this.defaultConsumerMaxRate = -1;
      this.defaultProducerWindowSize = 1000;
      this.defaultProducerMaxRate = -1;
      this.dispatcher = new PacketDispatcherImpl();
   }

   public ClientConnection createConnection() throws MessagingException
   {
      return createConnection(null, null);
   }
   
   public ClientConnection createConnection(final String username, final String password) throws MessagingException
   {
      int v = serverVersion.getIncrementingVersion();
                       
      RemotingConnection remotingConnection = null;
      try
      {
         remotingConnection = new RemotingConnectionImpl(remotingConfig, dispatcher);
       
         remotingConnection.start();
         
         String sessionID = remotingConnection.getSessionID();
         
         CreateConnectionRequest request =
            new CreateConnectionRequest(v, sessionID, JMSClientVMIdentifier.instance, username, password);
         
         CreateConnectionResponse response =
            (CreateConnectionResponse)remotingConnection.send(id, request);
         
         ClientConnectionImpl connection =
            new ClientConnectionImpl(response.getConnectionID(), serverID, strictTck, remotingConnection,
            		defaultConsumerWindowSize, defaultConsumerMaxRate,
            		defaultProducerWindowSize, defaultProducerMaxRate);

         return connection;
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
   
   public RemotingConfiguration getRemotingConfiguration()
   {
      return remotingConfig;
   }
   
   public Version getServerVersion()
   {
      return serverVersion;
   }

	public int getConsumerWindowSize()
	{
		return defaultConsumerWindowSize;
	}

	public int getProducerWindowSize()
	{
		return defaultProducerWindowSize;
	}

	public int getServerID()
	{
		return serverID;
	}

	public boolean isStrictTck()
	{
		return strictTck;
	}

	public int getMaxProducerRate()
	{
		return defaultProducerMaxRate;
	}
	
	
   
   // Public ---------------------------------------------------------------------------------------
      
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   // Inner Classes --------------------------------------------------------------------------------
}
