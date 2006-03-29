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

import java.util.Map;

import javax.jms.JMSException;

import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.endpoint.ServerConnectionEndpoint;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.Util;
import org.jboss.remoting.Client;
import org.jboss.remoting.ClientDisconnectedException;
import org.jboss.remoting.ConnectionListener;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentReaderHashMap;

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

   protected Map connections;

   // Constructors --------------------------------------------------

   public SimpleConnectionManager()
   {
      connections = new ConcurrentReaderHashMap();
   }

   // ConnectionManager ---------------------------------------------

   public void registerConnection(String remotingClientSessionID, ServerConnectionEndpoint endpoint)
   {    
      connections.put(remotingClientSessionID, endpoint);

      log.debug("registered connection " + endpoint + " as " +
                Util.guidToString(remotingClientSessionID));
   }

   public ServerConnectionEndpoint unregisterConnection(String remotingClientSessionID)
   {
      ServerConnectionEndpoint e =
         (ServerConnectionEndpoint)connections.remove(remotingClientSessionID);

      log.debug("unregistered connection " + e + " with remoting session ID " +
                Util.guidToString(remotingClientSessionID));
      return e;
   }

   public ServerConnectionEndpoint getConnection(String remotingClientSessionID)
   {
      return (ServerConnectionEndpoint)connections.get(remotingClientSessionID);
   }

   // ConnectionListener --------------------------------------------

   /**
    * Be aware that ConnectionNotifier uses to call this method with null Throwables.
    * @param t - expect it to be null!
    */
   public void handleConnectionException(Throwable t, Client client)
   {  
      String remotingSessionID = client.getSessionId();

      if (t instanceof ClientDisconnectedException)
      {
         // This is OK
         if (log.isTraceEnabled()) { log.trace(this + " notified that client " + client + " has disconnected"); }
         return;
      }
      
      ServerConnectionEndpoint ce = (ServerConnectionEndpoint)connections.remove(remotingSessionID);

      if (ce == null)
      {
         //Not all remoting sessions correspond to jms connections so this is ok to ignore         
         return;
      }
      
      log.warn(this + " handling client " + remotingSessionID + "'s remoting connection failure " +
            "(" + (t == null ? "null Throwable" : t.getClass().toString()) + ")", t);

      try
      {
         ce.close();
         log.debug("cleared up state for connection " + ce);
      }
      catch (JMSException e)
      {
         log.error("Failed to close connection", e);
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
