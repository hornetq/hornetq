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

import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.connectionfactory.JNDIBindings;
import org.jboss.jms.server.endpoint.advised.ConnectionAdvised;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.util.ExceptionUtil;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.IdBlock;

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

   private ServerPeer serverPeer;
   
   private String clientID;
   
   private int id;
   
   private JNDIBindings jndiBindings;
   
   private int prefetchSize;
   
   protected int defaultTempQueueFullSize;
   
   protected int defaultTempQueuePageSize;
   
   protected int defaultTempQueueDownCacheSize;

 
   // Constructors --------------------------------------------------

   /**
    * @param jndiBindings - names under which the corresponding JBossConnectionFactory is bound in
    *        JNDI.
    */
   public ServerConnectionFactoryEndpoint(int id, ServerPeer serverPeer,
                                          String defaultClientID,
                                          JNDIBindings jndiBindings,
                                          int preFetchSize,
                                          int defaultTempQueueFullSize,
                                          int defaultTempQueuePageSize,
                                          int defaultTempQueueDownCacheSize)
   {
      this.serverPeer = serverPeer;
      this.clientID = defaultClientID;
      this.id = id;
      this.jndiBindings = jndiBindings;
      this.prefetchSize = preFetchSize;
      this.defaultTempQueueFullSize = defaultTempQueueFullSize;
      this.defaultTempQueuePageSize = defaultTempQueuePageSize;
      this.defaultTempQueueDownCacheSize = defaultTempQueueDownCacheSize;
   }

   // ConnectionFactoryDelegate implementation ----------------------
   
   public ConnectionDelegate createConnectionDelegate(String username, String password)
      throws JMSException
   {
      try
      {
         log.debug("creating a new connection for user " + username);
         
         // authenticate the user
         serverPeer.getSecurityManager().authenticate(username, password);
         
         // see if there is a preconfigured client id for the user
         if (username != null)
         {
            
            
            String preconfClientID =
               serverPeer.getJmsUserManagerInstance().getPreConfiguredClientID(username);
            
            if (preconfClientID != null)
            {
               clientID = preconfClientID;
            }
         }
   
         // create the corresponding "server-side" connection endpoint and register it with the
         // server peer's ClientManager
         ServerConnectionEndpoint endpoint =
            new ServerConnectionEndpoint(serverPeer, clientID, username, password, prefetchSize,
                     defaultTempQueueFullSize, defaultTempQueuePageSize, defaultTempQueueDownCacheSize);
   
         int connectionID = endpoint.getConnectionID();
   
         ConnectionAdvised connAdvised = new ConnectionAdvised(endpoint);
         JMSDispatcher.instance.registerTarget(new Integer(connectionID), connAdvised);
         
         log.debug("created and registered " + endpoint);
   
         return new ClientConnectionDelegate(connectionID);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createConnectionDelegate");
      }
   }
   
   public byte[] getClientAOPConfig() throws JMSException
   {
      try
      {
         return serverPeer.getClientAOPConfig();
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " getClientAOPConfig");
      }
   }

   public IdBlock getIdBlock(int size) throws JMSException
   {
      try
      {
         return serverPeer.getMessageIdManager().getIdBlock(size);
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " getIdBlock");
      }
   }


   // Public --------------------------------------------------------
   
   public int getID()
   {
      return id;
   }

   public JNDIBindings getJNDIBindings()
   {
      return jndiBindings;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
