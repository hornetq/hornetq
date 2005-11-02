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
package org.jboss.jms.server;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;

import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.DurableSubscription;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

/**
 * Manages client connections. There is a single ClientManager instance for each server peer.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientManager
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ClientManager.class);


   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ServerPeer serverPeer;
   protected Map connections;
   protected Map subscriptions;
   
   // Constructors --------------------------------------------------

   public ClientManager(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
      connections = new ConcurrentReaderHashMap();
      subscriptions = new ConcurrentReaderHashMap();

   }

   // Public --------------------------------------------------------
   
   public void stop()
   {
      //checker.stop();
   }

   public ServerConnectionDelegate putConnectionDelegate(Serializable connectionID,
                                                         ServerConnectionDelegate d)
   {
      return (ServerConnectionDelegate)connections.put(connectionID, d);
   }
   
   public void removeConnectionDelegate(Serializable connectionID)
   {
      connections.remove(connectionID);
   }

   public ServerConnectionDelegate getConnectionDelegate(Serializable connectionID)
   {
      return (ServerConnectionDelegate)connections.get(connectionID);
   }
   
   public Set getDurableSubscriptions(String clientID)
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs != null ? new HashSet(subs.values()) : Collections.EMPTY_SET;
   }
   
   public DurableSubscription getDurableSubscription(String clientID, String subscriptionName)
   {
      Map subs = (Map)subscriptions.get(clientID);
      return subs == null ? null : (DurableSubscription)subs.get(subscriptionName);
   }
   
   public void addDurableSubscription(String clientID, String subscriptionName,
         DurableSubscription subscription)
   {
      Map subs = (Map)subscriptions.get(clientID);
      if (subs == null)
      {
         subs = new ConcurrentReaderHashMap();
         subscriptions.put(clientID, subs);
      }
      subs.put(subscriptionName, subscription);
   }
   
   public DurableSubscription removeDurableSubscription(String clientID,
                                                              String subscriptionName)
      throws JMSException
   {
      if (clientID == null)
      {
         throw new JMSException("Client ID must be set for connection!");
      }
      
      Map subs = (Map)subscriptions.get(clientID);
      
      if (subs == null)
      {
         return null;
      }
                 
      
      if (log.isTraceEnabled()) { log.trace("Removing durable sub: " + subscriptionName); }
      
      DurableSubscription removed = (DurableSubscription)subs.remove(subscriptionName);
      
      if (subs.size() == 0)
      {
         subscriptions.remove(clientID);
      }
      
      return removed;
   }
   
  

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
