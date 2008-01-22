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
import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.Version;

/**
 * The client-side ConnectionFactory delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision: 3602 $</tt>
 *
 * $Id: ClientConnectionFactoryImpl.java 3602 2008-01-21 17:48:32Z timfox $
 */
public class ClientConnectionFactoryImpl implements Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;
   
   private static final Logger log = Logger.getLogger(ClientConnectionFactoryImpl.class);

   // Attributes -----------------------------------------------------------------------------------

   private String id;
   
   private String serverLocatorURI;

   private Version serverVersion;
 
   private int serverID;
   
   private boolean clientPing;

   private boolean strictTck;
   
   // Static ---------------------------------------------------------------------------------------
   
   private static Version getVersionToUse(Version connectionVersion)
   {
      Version clientVersion = Version.instance();

      Version versionToUse;

      if (connectionVersion.getProviderIncrementingVersion() <=
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

   // Constructors ---------------------------------------------------------------------------------

   public ClientConnectionFactoryImpl(String id, int serverID, 
         String serverLocatorURI, Version serverVersion, boolean clientPing, boolean strictTck)
   {
      this.id = id;
      this.serverID = serverID;
      this.serverLocatorURI = serverLocatorURI;
      this.serverVersion = serverVersion;
      this.clientPing = clientPing;
      this.strictTck = strictTck;
   }
   
   public ClientConnectionFactoryImpl()
   {
   }
   
   public ClientConnection createConnection(String username, String password) throws JMSException
   {
      Version version = getVersionToUse(serverVersion);
      
      byte v = version.getProviderIncrementingVersion();
                       
      MessagingRemotingConnection remotingConnection = null;
      try
      {
         remotingConnection = new MessagingRemotingConnection(version, serverLocatorURI);
       
         remotingConnection.start();
         
         String sessionID = remotingConnection.getSessionID();
         
         CreateConnectionRequest request =
            new CreateConnectionRequest(v, sessionID, JMSClientVMIdentifier.instance, username, password);
         
         CreateConnectionResponse response =
            (CreateConnectionResponse)remotingConnection.sendBlocking(id, request);
         
         ResourceManager resourceManager = ResourceManagerFactory.instance.checkOutResourceManager(this.serverID);
            
         ClientConnectionImpl connection =
            new ClientConnectionImpl(response.getConnectionID(), serverID, strictTck, version, resourceManager, remotingConnection);
         
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
   
   // Public ---------------------------------------------------------------------------------------

   public String getServerLocatorURI()
   {
      return serverLocatorURI;
   }
   
   public int getServerID()
   {
      return serverID;
   }
   
   public boolean getClientPing()
   {
      return clientPing;
   }
   
   public Version getServerVersion()
   {
      return serverVersion;
   }
   
   public boolean getStrictTck()
   {
       return strictTck;
   }

   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   // Inner Classes --------------------------------------------------------------------------------

}
