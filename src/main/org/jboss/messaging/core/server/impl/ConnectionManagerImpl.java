/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.server.impl;

import org.jboss.messaging.core.client.RemotingSessionListener;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.ConnectionManager;
import org.jboss.messaging.core.server.ServerConnection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision: 3778 $</tt>
 *
 * $Id: ConnectionManagerImpl.java 3778 2008-02-24 12:15:29Z timfox $
 */
public class ConnectionManagerImpl implements ConnectionManager, RemotingSessionListener
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionManagerImpl.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private Map<Long /* remoting session ID */, List<ServerConnection>> connections;
   
   // Constructors ---------------------------------------------------------------------------------

   public ConnectionManagerImpl()
   {
      connections = new HashMap<Long, List<ServerConnection>>();
   }

   // ConnectionManager implementation -------------------------------------------------------------

   public synchronized void registerConnection(final long remotingClientSessionID,
                                               final ServerConnection connection)
   {    
      List<ServerConnection> connectionEndpoints = connections.get(remotingClientSessionID);

      if (connectionEndpoints == null)
      {
         connectionEndpoints = new ArrayList<ServerConnection>();
         connections.put(remotingClientSessionID, connectionEndpoints);
      }

      connectionEndpoints.add(connection);

      log.debug("registered connection " + connection + " as " + remotingClientSessionID);
   }
   
   public synchronized ServerConnection unregisterConnection(final long remotingClientSessionID,
                                                             final ServerConnection connection)
   {
      List<ServerConnection> connectionEndpoints = connections.get(remotingClientSessionID);

      if (connectionEndpoints != null)
      {
         connectionEndpoints.remove(connection);

         log.debug("unregistered connection " + connection + " with remoting session ID " + remotingClientSessionID);

         if (connectionEndpoints.isEmpty())
         {
            connections.remove(remotingClientSessionID);           
         }

         return connection;
      }
      return null;
   }
   
   public synchronized List<ServerConnection> getActiveConnections()
   {
      // I will make a copy to avoid ConcurrentModification
      List<ServerConnection> list = new ArrayList<ServerConnection>();
      for (List<ServerConnection> conns : connections.values())
      {
         list.addAll(conns);
      }
      return list;
   }      
   
   public synchronized int size()
   {
      int size = 0;
      for (List<ServerConnection> conns : connections.values())
      {
         size += conns.size();
      }
      return size;
   }

   // FailureListener implementation --------------------------------------------------------------

   public void sessionDestroyed(long sessionID, MessagingException me)
   {
      if (me != null)
      {
         log.warn(me.getMessage(), me);
      }
      closeConnections(sessionID);
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConnectionManager[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private synchronized void closeConnections(final long remotingClientSessionID)
   {
      List<ServerConnection> conns = connections.get(remotingClientSessionID);
      
      if (conns == null || conns.isEmpty())
      {
         return;
      }
      
      // we still have connections open for the session
      
      log.warn("A problem has been detected with the connection from client " +
            remotingClientSessionID + ". It is possible the client has exited without closing " +
            "its connection(s) or the network has failed. All connection resources " +
            "corresponding to that client process will now be removed.");

      // the connection connections are copied in a new list to avoid concurrent modification exception
      List<ServerConnection> copy;
      if (conns != null)
      {
         copy = new ArrayList<ServerConnection>(conns);
      }
      else
      {
         copy = new ArrayList<ServerConnection>();
      }
         
      for (ServerConnection sce : copy)
      {
         try
         {
            log.debug("clearing up state for connection " + sce);
            sce.close();
            log.debug("cleared up state for connection " + sce);
         }
         catch (Exception e)
         {
            log.error("Failed to close connection", e);
         }          
      }
      conns.clear();
      connections.remove(remotingClientSessionID);
   }

}
