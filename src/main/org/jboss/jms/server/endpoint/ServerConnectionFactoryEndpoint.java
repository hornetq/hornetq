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
package org.jboss.jms.server.endpoint;

import javax.jms.JMSException;

import org.jboss.aop.Dispatcher;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.advised.ConnectionAdvised;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;

/**
 * Concrete implementation of ConnectionFactoryEndpoint
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConnectionFactoryEndpoint implements ConnectionFactoryEndpoint
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConnectionFactoryEndpoint.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   
   protected String clientID;
   
   protected String id;

   // Constructors --------------------------------------------------

   public ServerConnectionFactoryEndpoint(String id, ServerPeer serverPeer, String defaultClientID)
   {
      this.serverPeer = serverPeer;
      
      this.clientID = defaultClientID;
      
      this.id = id;
   }

   // ConnectionFactoryDelegate implementation ----------------------

   public ConnectionDelegate createConnectionDelegate(String username, String password,
                                                      String clientConnectionId) throws JMSException
   {
      log.debug("Creating a new connection with username=" + username);
      
      //authenticate the user
      serverPeer.getSecurityManager().authenticate(username, password);
    
      //See if there is a preconfigured client id for the user
      if (username != null)
      {
         String preconfClientID =
            serverPeer.getDurableSubscriptionStoreDelegate().getPreConfiguredClientID(username);
         
         if (preconfClientID != null)
         {
            clientID = preconfClientID;
         }
      }

      // create the corresponding "server-side" connection endpoint and register it with the
      // server peer's ClientManager
      ServerConnectionEndpoint endpoint =
         new ServerConnectionEndpoint(serverPeer, clientID, username, password, clientConnectionId);


      String connectionID = endpoint.getConnectionID();

      serverPeer.getClientManager().putConnectionDelegate(connectionID, endpoint);
      ConnectionAdvised connAdvised = new ConnectionAdvised(endpoint);
      Dispatcher.singleton.registerTarget(connectionID, connAdvised);
      
      serverPeer.registerConnection(clientConnectionId, endpoint);

      log.debug("created and registered " + endpoint);

      ClientConnectionDelegate delegate;
      try
      {
         delegate = new ClientConnectionDelegate(connectionID,                                                 
                                                 serverPeer.getServerPeerID(),
                                                 serverPeer.getVersion());
      }
      catch (Exception e)
      {
         throw new JBossJMSException("Failed to create connection stub", e);
      }  
      
      return delegate;
   }
   
   public byte[] getClientAOPConfig()
   {
      return serverPeer.getClientAOPConfig();
   }

   // Public --------------------------------------------------------
   
   public String getID()
   {
      return id;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
