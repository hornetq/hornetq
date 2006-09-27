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
package org.jboss.jms.server.connectionfactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.jms.server.endpoint.advised.ConnectionFactoryAdvised;
import org.jboss.jms.server.remoting.JMSDispatcher;
import org.jboss.jms.util.JNDIUtil;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryJNDIMapper implements ConnectionFactoryManager
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionFactoryJNDIMapper.class);
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Context initialContext;
   protected ServerPeer serverPeer;
   protected Map endpoints;
   protected Map factories;

   // Constructors --------------------------------------------------

   public ConnectionFactoryJNDIMapper(ServerPeer serverPeer) throws Exception
   {
      this.serverPeer = serverPeer;
      endpoints = new HashMap();
      factories = new HashMap();
   }

   // ConnectionFactoryManager implementation -----------------------

   public synchronized int registerConnectionFactory(String clientID,
                                                     JNDIBindings jndiBindings,
                                                     String locatorURI,
                                                     boolean clientPing,
                                                     int prefetchSize,
                                                     int defaultTempQueueFullSize,
                                                     int defaultTempQueuePageSize,
                                                     int defaultTempQueueDownCacheSize) throws Exception
   {
      int id = serverPeer.getNextObjectID();

      ServerConnectionFactoryEndpoint endpoint =
         new ServerConnectionFactoryEndpoint(id, serverPeer, clientID, jndiBindings,
                                             prefetchSize, defaultTempQueueFullSize,
                                             defaultTempQueuePageSize,
                                             defaultTempQueueDownCacheSize);

      ClientConnectionFactoryDelegate delegate =
         new ClientConnectionFactoryDelegate(id, locatorURI, serverPeer.getVersion(),
                                             serverPeer.getServerPeerID(), clientPing);

      ConnectionFactoryAdvised connFactoryAdvised = new ConnectionFactoryAdvised(endpoint);
      
      JMSDispatcher.instance.registerTarget(new Integer(id), connFactoryAdvised);

      endpoints.put(new Integer(id), endpoint);

      JBossConnectionFactory cf = new JBossConnectionFactory(delegate);

      if (jndiBindings != null)
      {
         List jndiNames = jndiBindings.getNames();
         for(Iterator i = jndiNames.iterator(); i.hasNext(); )
         {
            String jndiName = (String)i.next();
            JNDIUtil.rebind(initialContext, jndiName, cf);
         }
      }

      factories.put(new Integer(id), cf);

      return id;
   }

   public synchronized void unregisterConnectionFactory(int connFactoryID) throws Exception
   {
      ServerConnectionFactoryEndpoint endpoint =
         (ServerConnectionFactoryEndpoint)endpoints.get(new Integer(connFactoryID));

      JNDIBindings jndiBindings = endpoint.getJNDIBindings();
      if (jndiBindings != null)
      {
         List jndiNames = jndiBindings.getNames();
         for(Iterator i = jndiNames.iterator(); i.hasNext(); )
         {
            String jndiName = (String)i.next();
            initialContext.unbind(jndiName);
            log.debug(jndiName + " unregistered");
         }
      }
      JMSDispatcher.instance.unregisterTarget(new Integer(connFactoryID));
   }

   public synchronized javax.jms.ConnectionFactory getConnectionFactory(int connectionFactoryID)
   {
      return (javax.jms.ConnectionFactory)factories.get(new Integer(connectionFactoryID));
   }
   
   // MessagingComponent implementation -----------------------------
   
   public void start() throws Exception
   {
      initialContext = new InitialContext();
      log.debug("started");
   }

   public void stop() throws Exception
   {
      initialContext.close();
      log.debug("stopped");
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
