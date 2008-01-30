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
package org.jboss.jms.client.impl;

import java.io.Serializable;

import javax.jms.JMSException;

import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.api.ClientConnectionFactory;
import org.jboss.jms.client.plugin.LoadBalancingFactory;
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.messaging.core.remoting.RemotingConfiguration;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.Version;

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
   
   private RemotingConfiguration remotingConfig;

   private Version serverVersion;
 
   private int serverID;
   
   private String clientID;

   private int prefetchSize = 150;

   private boolean supportsFailover;

   private boolean supportsLoadBalancing;

   private LoadBalancingFactory loadBalancingFactory;

   private int dupsOKBatchSize = 1000;

   private boolean strictTck;
   
   // Static ---------------------------------------------------------------------------------------
    
   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionFactoryImpl(int serverID,
         RemotingConfiguration remotingConfig, Version serverVersion, boolean strictTck,
         int prefetchSize, int dupsOKBatchSize, String clientID)
   {
      this.serverID = serverID;
      this.remotingConfig = remotingConfig;
      this.serverVersion = serverVersion;
      this.strictTck = strictTck;
      this.prefetchSize = prefetchSize;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.clientID = clientID;
   }

   public ClientConnectionFactoryImpl(RemotingConfiguration remotingConfig)
   {
      this.remotingConfig = remotingConfig;
   }

   public ClientConnectionFactoryImpl()
   {
   }
   
   public ClientConnection createConnection() throws JMSException
   {
      return createConnection(null, null);
   }
   
   public ClientConnection createConnection(String username, String password) throws JMSException
   {
      Version version = getVersionToUse(serverVersion);
      
      byte v = version.getProviderIncrementingVersion();
                       
      MessagingRemotingConnection remotingConnection = null;
      try
      {
         remotingConnection = new MessagingRemotingConnection(remotingConfig);
       
         remotingConnection.start();
         
         String sessionID = remotingConnection.getSessionID();
         
         CreateConnectionRequest request =
            new CreateConnectionRequest(v, sessionID, JMSClientVMIdentifier.instance, username, password,
                  prefetchSize, dupsOKBatchSize, clientID);
         
         CreateConnectionResponse response =
            (CreateConnectionResponse)remotingConnection.send(id, request);
         
         ClientConnectionImpl connection =
            new ClientConnectionImpl(response.getConnectionID(), serverID, strictTck, version, remotingConnection);
         
         //FIXME - get rid of this stupid ConsolidatedThingamajug bollocks
         
         ConsolidatedRemotingConnectionListener listener = new ConsolidatedRemotingConnectionListener(connection);
         
         remotingConnection.addConnectionListener(listener);
         
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
         
         //TODO - we will sort out exception handling further in the refactoring
         
         log.error("Failed to start connection ", t);
         
         throw new MessagingJMSException("Failed to start connection", t);
      }
   }
   
   // ClientConnectionFactory implementation ---------------------------------------------
   
   public RemotingConfiguration getRemotingConfiguration()
   {
      return remotingConfig;
   }
   
   public int getServerID()
   {
      return serverID;
   }
   
   public Version getServerVersion()
   {
      return serverVersion;
   }
   
   // Public ---------------------------------------------------------------------------------------
      
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private Version getVersionToUse(Version connectionVersion)
   {
      Version clientVersion = Version.instance();

      Version versionToUse;

      if (connectionVersion != null && connectionVersion.getProviderIncrementingVersion() <=
          clientVersion.getProviderIncrementingVersion())
      {
         versionToUse = connectionVersion;
      }
      else
      {
         versionToUse = clientVersion;
      }

      return versionToUse;
   }
   
   // Inner Classes --------------------------------------------------------------------------------

}
