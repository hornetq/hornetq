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

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.jms.util.JBossJMSException;
import org.jboss.jms.server.ConnectionFactoryManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.logging.Logger;
import org.jboss.aop.Dispatcher;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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

   public synchronized String registerConnectionFactory(String clientID,
                                                        JNDIBindings jndiBindings) throws Exception
   {
      String id = generateConnectionFactoryID();

      ServerConnectionFactoryEndpoint endpoint =
         new ServerConnectionFactoryEndpoint(id, serverPeer, clientID, jndiBindings);

      ClientConnectionFactoryDelegate delegate;
      try
      {
         delegate = new ClientConnectionFactoryDelegate(id, serverPeer.getLocatorURI());
      }
      catch (Exception e)
      {
         throw new JBossJMSException("Failed to create connection factory delegate", e);
      }

      Dispatcher.singleton.registerTarget(id, endpoint);

      endpoints.put(id, endpoint);

      JBossConnectionFactory cf = new JBossConnectionFactory(delegate);

      if (jndiBindings != null)
      {
         List jndiNames = jndiBindings.getNames();
         for(Iterator i = jndiNames.iterator(); i.hasNext(); )
         {
            String jndiName = (String)i.next();
            initialContext.rebind(jndiName, cf);
         }
      }

      factories.put(id, cf);

      return id;
   }

   public synchronized void unregisterConnectionFactory(String connFactoryID) throws Exception
   {
      ServerConnectionFactoryEndpoint endpoint =
         (ServerConnectionFactoryEndpoint)endpoints.get(connFactoryID);

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
   }

   public synchronized javax.jms.ConnectionFactory getConnectionFactory(String connectionFactoryID)
   {
      return (javax.jms.ConnectionFactory)factories.get(connectionFactoryID);
   }

   // Public --------------------------------------------------------

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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private long connFactoryIDSequence = 0;

   private synchronized String generateConnectionFactoryID()
   {
      return "CONNFACTORY_" + connFactoryIDSequence++;
   }

   // Inner classes -------------------------------------------------
}
