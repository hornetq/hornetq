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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONN_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONN_START;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONN_STOP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.jms.server.ConnectionManager;
import org.jboss.jms.server.SecurityStore;
import org.jboss.messaging.core.Binding;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.PostOffice;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.wireformat.ConnectionCreateSessionResponseMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.util.MessagingException;

/**
 * Concrete implementation of ConnectionEndpoint.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConnectionEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConnectionEndpoint.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------

   private final String id;

   private volatile boolean started;

   private final String username;
   
   private final String password;

   private final String remotingClientSessionID;
   
   private final String jmsClientVMID;

   private final MessagingServer messagingServer;

   private final PostOffice postOffice;
   
   private final SecurityStore sm;
   
   private final ConnectionManager cm;

   private final ConcurrentMap<String, ServerSessionEndpoint> sessions = new ConcurrentHashMap<String, ServerSessionEndpoint>();

   private final Set<Queue> temporaryQueues = new ConcurrentHashSet<Queue>();

   private final int prefetchSize;

   // Constructors ---------------------------------------------------------------------------------

   public ServerConnectionEndpoint(MessagingServer messagingServer,
                                   String username, String password, int prefetchSize,
                                   String remotingSessionID,
                                   String clientVMID) throws Exception
   {
      this.messagingServer = messagingServer;

      sm = messagingServer.getSecurityManager();
      cm = messagingServer.getConnectionManager();
      postOffice = messagingServer.getPostOffice();

      started = false;

      this.id = UUID.randomUUID().toString();
      
      this.prefetchSize = prefetchSize;

      this.username = username;
      
      this.password = password;

      this.remotingClientSessionID = remotingSessionID;

      this.jmsClientVMID = clientVMID;
      
      cm.registerConnection(jmsClientVMID, remotingClientSessionID, this);
   }

   // ConnectionDelegate implementation ------------------------------------------------------------

   public ConnectionCreateSessionResponseMessage createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks,
                                              PacketSender sender)
      throws Exception
   {           
      String sessionID = UUID.randomUUID().toString();
 
      ServerSessionEndpoint ep =
         new ServerSessionEndpoint(sessionID, this, autoCommitSends, autoCommitAcks, xa, sender, messagingServer.getResourceManager());            

      synchronized (sessions)
      {
         sessions.put(sessionID, ep);
      }

      messagingServer.getRemotingService().getDispatcher().register(ep.newHandler());
      
      return new ConnectionCreateSessionResponseMessage(sessionID);
   }
   
   public void start() throws Exception
   {
      setStarted(true);
   }

   public synchronized void stop() throws Exception
   {
      setStarted(false);
   }

   public void close() throws Exception
   {
      Map<String, ServerSessionEndpoint> sessionsClone = new HashMap<String, ServerSessionEndpoint>(sessions);
      
      for (ServerSessionEndpoint session: sessionsClone.values())
      {
         session.close();
      }

      sessions.clear();
      
      Set<String> addresses = new HashSet<String>();

      for (Queue tempQueue: temporaryQueues)
      {                        
         Binding binding = postOffice.getBinding(tempQueue.getName());
         
         addresses.add(binding.getAddress());     
         
         postOffice.removeBinding(tempQueue.getName());         
      }
      
      for (String address: addresses)
      {
         postOffice.removeAllowableAddress(address);
      }

      temporaryQueues.clear();      

      cm.unregisterConnection(remotingClientSessionID, this);

      messagingServer.getRemotingService().getDispatcher().unregister(id);
   }

   // Public ---------------------------------------------------------------------------------------

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }

   public SecurityStore getSecurityManager()
   {
      return sm;
   }

   public MessagingServer getMessagingServer()
   {
      return messagingServer;
   }

   public PacketHandler newHandler()
   {
      return new ConnectionPacketHandler();
   }

   public String toString()
   {
      return "ConnectionEndpoint[" + id + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   int getPrefetchSize()
   {
      return prefetchSize;
   }

   String getConnectionID()
   {
      return id;
   }

   boolean isStarted()
   {
      return started;
   }

   void removeSession(String sessionId) throws Exception
   {
      if (sessions.remove(sessionId) == null)
      {
         throw new IllegalStateException("Cannot find session with id " + sessionId + " to remove");
      }      
   }

   void addTemporaryQueue(Queue queue)
   {
      temporaryQueues.add(queue);      
   }
   
   void removeTemporaryQueue(Queue queue)
   {
      temporaryQueues.remove(queue);      
   }

   String getRemotingClientSessionID()
   {
      return remotingClientSessionID;
   }
  
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void setStarted(boolean started) throws Exception
   {
      Map<String, ServerSessionEndpoint> sessionsClone = null;
      
      sessionsClone = new HashMap<String, ServerSessionEndpoint>(sessions);
            
      for (ServerSessionEndpoint session: sessionsClone.values() )
      {
         session.setStarted(started);
      }
      
      this.started = started;      
   }   
    
   // Inner classes --------------------------------------------------------------------------------

   private class ConnectionPacketHandler extends ServerPacketHandlerSupport
   {
      public ConnectionPacketHandler()
      {
      }

      public String getID()
      {
         return ServerConnectionEndpoint.this.id;
      }

      public Packet doHandle(Packet packet, PacketSender sender) throws Exception
      {
         Packet response = null;

         PacketType type = packet.getType();
         
         if (type == CONN_CREATESESSION)
         {
            ConnectionCreateSessionMessage request = (ConnectionCreateSessionMessage) packet;
            
            response = createSession(request.isXA(), request.isAutoCommitSends(), request.isAutoCommitAcks(), sender);
         }
         else if (type == CONN_START)
         {
            start();
         }
         else if (type == CONN_STOP)
         {
            stop();
         }
         else if (type == CLOSE)
         {
            close();
         }                       
         else
         {
            throw new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                                         "Unsupported packet " + type);
         }

         // reply if necessary
         if (response == null && packet.isOneWay() == false)
         {
            response = new NullPacket();               
         }
         
         return response;
      }

      @Override
      public String toString()
      {
         return "ConnectionAdvisedPacketHandler[id=" + id + "]";
      }
   }

}
