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
package org.jboss.jms.server.connectionmanager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jms.JMSException;

import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Util;
import org.jboss.remoting.Client;
import org.jboss.remoting.ClientDisconnectedException;
import org.jboss.remoting.ConnectionListener;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version 1.1
 *
 * SimpleConnectionManager.java,v 1.1 2006/02/21 07:44:00 timfox Exp
 */
public class SimpleConnectionManager implements ConnectionManager, ConnectionListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SimpleConnectionManager.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Map jmsClients;
   
   protected Map sessions;
   
   // Constructors --------------------------------------------------

   public SimpleConnectionManager()
   {
      jmsClients = new HashMap();
      
      sessions = new HashMap();
   }

   // ConnectionManager ---------------------------------------------

   public synchronized void registerConnection(String jmsClientId, String remotingClientSessionID, ServerConnectionEndpoint endpoint)
   {    
      Map endpoints = (Map)jmsClients.get(jmsClientId);
      
      if (endpoints == null)
      {
         endpoints = new HashMap();
         jmsClients.put(jmsClientId, endpoints);                  
      }
      
      endpoints.put(remotingClientSessionID, endpoint);
      
      sessions.put(remotingClientSessionID, jmsClientId);
      
      log.debug("registered connection " + endpoint + " as " +
                Util.guidToString(remotingClientSessionID));
   }

   public synchronized ServerConnectionEndpoint unregisterConnection(String jmsClientId, String remotingClientSessionID)
   {
      Map endpoints = (Map)jmsClients.get(jmsClientId);
      
      if (endpoints != null)
      {
         ServerConnectionEndpoint e =
            (ServerConnectionEndpoint)endpoints.remove(remotingClientSessionID);
         
         log.debug("unregistered connection " + e + " with remoting session ID " +
               Util.guidToString(remotingClientSessionID));
         
         if (endpoints.isEmpty())
         {
            jmsClients.remove(jmsClientId);
         }
         
         sessions.remove(remotingClientSessionID);
         
         return e;
      }
      return null;
   }
   
   public boolean containsSession(String remotingClientSessionID)
   {
      return sessions.containsKey(remotingClientSessionID);
   }

   // ConnectionListener --------------------------------------------

   /**
    * Be aware that ConnectionNotifier uses to call this method with null Throwables.
    * @param t - expect it to be null!
    */
   public synchronized void handleConnectionException(Throwable t, Client client)
   {  
      String remotingSessionID = client.getSessionId();

      if (t instanceof ClientDisconnectedException)
      {
         // This is OK
         if (log.isTraceEnabled()) { log.trace(this + " notified that client " + client + " has disconnected"); }
         return;
      }
      
      String jmsClientId = (String)sessions.get(remotingSessionID);
      
      if (jmsClientId != null)
      {
         log.warn(this + " handling client " + remotingSessionID + "'s remoting connection failure " +
               "(" + (t == null ? "null Throwable" : t.getClass().toString()) + ")", t);
         
         //Remoting only provides one pinger per invoker, not per connection therefore when the pinger dies
         //we must close ALL the connections corresponding to that jms client id
         Map endpoints = (Map)jmsClients.get(jmsClientId);
         
         if (endpoints != null)
         {
            Iterator iter = endpoints.entrySet().iterator();
            
            while (iter.hasNext())
            {
               Map.Entry entry = (Map.Entry)iter.next();
               
               String sessionId = (String)entry.getKey();
               
               ServerConnectionEndpoint sce = (ServerConnectionEndpoint)entry.getValue();
               
               try
               {
                  sce.close();
                  log.debug("cleared up state for connection " + sce);
               }
               catch (JMSException e)
               {
                  log.error("Failed to close connection", e);
               }
               
               sessions.remove(sessionId);
            }
            
            jmsClients.remove(jmsClientId);
         }
      } 
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "ConnectionManager[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
