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


import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UPDATECALLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONNECTION;

import java.util.Map;

import javax.jms.JMSException;

import org.jboss.jms.client.impl.ClientConnectionFactoryImpl;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.UpdateCallbackMessage;
import org.jboss.messaging.util.ExceptionUtil;
import org.jboss.messaging.util.Logger;

/**
 * Concrete implementation of ConnectionFactoryEndpoint
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServerConnectionFactoryEndpoint implements ConnectionFactoryEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConnectionFactoryEndpoint.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private MessagingServer messagingServer;

   private String clientID;

   private String uniqueName;

   private String id;

   private int prefetchSize;

   private int defaultTempQueueFullSize;

   private int defaultTempQueuePageSize;

   private int defaultTempQueueDownCacheSize;

   private int dupsOKBatchSize;
     
   /** Cluster Topology on ClusteredConnectionFactories
       Information to failover to other connections on clients **/
   ClientConnectionFactoryImpl[] delegates;

   /** Cluster Topology on ClusteredConnectionFactories
       Information to failover to other connections on clients **/
   Map failoverMap;

   
   // Constructors ---------------------------------------------------------------------------------
   public ServerConnectionFactoryEndpoint(String uniqueName, String id, MessagingServer messagingServer,
                                          String defaultClientID,
                                          int preFetchSize,
                                          int defaultTempQueueFullSize,
                                          int defaultTempQueuePageSize,
                                          int defaultTempQueueDownCacheSize,
                                          int dupsOKBatchSize)
   {
      this.uniqueName = uniqueName;
      this.messagingServer = messagingServer;
      this.clientID = defaultClientID;
      this.id = id;
      this.prefetchSize = preFetchSize;
      this.defaultTempQueueFullSize = defaultTempQueueFullSize;
      this.defaultTempQueuePageSize = defaultTempQueuePageSize;
      this.defaultTempQueueDownCacheSize = defaultTempQueueDownCacheSize;
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   // ConnectionFactoryDelegate implementation -----------------------------------------------------

   public CreateConnectionResponse createConnectionDelegate(String username,
                                                          String password,                                                          
                                                          String remotingSessionID,
                                                          String clientVMID,
                                                          byte versionToUse)
      throws JMSException      
   {
      try
      {
         return
            createConnectionDelegateInternal(username, password, 
                                             remotingSessionID, clientVMID,
                                             versionToUse);        
      }
      catch (Throwable t)
      {
         throw ExceptionUtil.handleJMSInvocation(t, this + " createFailoverConnectionDelegate");
      }
      
   }

   private CreateConnectionResponse
      createConnectionDelegateInternal(String username,
                                       String password,                                       
                                       String remotingSessionID, String clientVMID,
                                       byte versionToUse)
      throws Exception
   {
      log.trace("creating a new connection for user " + username);

      // Authenticate. Successful autentication will place a new SubjectContext on thread local,
      // which will be used in the authorization process. However, we need to make sure we clean
      // up thread local immediately after we used the information, otherwise some other people
      // security my be screwed up, on account of thread local security stack being corrupted.

      messagingServer.getSecurityManager().authenticate(username, password);

      // We don't need the SubjectContext on thread local anymore, clean it up
      SecurityActions.popSubjectContext();

      String clientIDUsed = clientID;

      // see if there is a preconfigured client id for the user
      if (username != null)
      {
         String preconfClientID =
            messagingServer.getJmsUserManagerInstance().getPreConfiguredClientID(username);

         if (preconfClientID != null)
         {
            clientIDUsed = preconfClientID;
         }                  
      }

      // create the corresponding "server-side" connection endpoint and register it with the
      // server peer's ClientManager
      final ServerConnectionEndpoint endpoint =
         new ServerConnectionEndpoint(messagingServer, clientIDUsed, username, password, prefetchSize,
                                      defaultTempQueueFullSize, defaultTempQueuePageSize,
                                      defaultTempQueueDownCacheSize, this,
                                      remotingSessionID, clientVMID, versionToUse,
                                      dupsOKBatchSize);

      String connectionID = endpoint.getConnectionID();

      messagingServer.getMinaService().getDispatcher().register(endpoint.newHandler());

      log.trace("created and registered " + endpoint);

      return new CreateConnectionResponse(connectionID);
   }
      
   public void addSender(String VMID, String remotingSessionID,
         PacketSender sender) throws JMSException
   {
      log.debug("Adding PacketSender on ConnectionFactory");
      messagingServer.getConnectionManager().addConnectionFactoryCallback(this.uniqueName, VMID, remotingSessionID, sender);
   }
   
   public void removeSender(String VMID, String remotingSessionID,
         PacketSender sender) throws JMSException
   {
      log.debug("Removing PacketSender on ConnectionFactory");
      messagingServer.getConnectionManager().removeConnectionFactoryCallback(this.uniqueName, VMID, sender);
   }

   // Public ---------------------------------------------------------------------------------------
   
   public String getID()
   {
      return id;
   }

   public MessagingServer getMessagingServer()
   {
      return messagingServer;
   }

   public String toString()
   {
      return "ConnectionFactoryEndpoint[" + id + "]";
   }

   public PacketHandler newHandler()
   {
      return new ConnectionFactoryAdvisedPacketHandler();
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------


   private final class ConnectionFactoryAdvisedPacketHandler implements
           PacketHandler
   {
      public String getID()
      {
         return ServerConnectionFactoryEndpoint.this.id;
      }

      public void handle(AbstractPacket packet, PacketSender sender)
      {
         try
         {
            AbstractPacket response = null;

            PacketType type = packet.getType();
            if (type == REQ_CREATECONNECTION)
            {
               CreateConnectionRequest request = (CreateConnectionRequest) packet;
               response = createConnectionDelegate(request
                     .getUsername(), request.getPassword(), request.getRemotingSessionID(),
                     request.getClientVMID(), request.getVersion());
            }
            else if (type == MSG_UPDATECALLBACK)
            {
               UpdateCallbackMessage message = (UpdateCallbackMessage) packet;
               if (message.isAdd())
               {
                  addSender(message.getClientVMID(), message.getRemotingSessionID(), sender);
               } else {
                  removeSender(message.getClientVMID(), message.getRemotingSessionID(), sender);
               }
            } else
            {
               response = new JMSExceptionMessage(new MessagingJMSException(
                     "Unsupported packet for browser: " + packet));
            }

            // reply if necessary
            if (response != null)
            {
               response.normalize(packet);
               sender.send(response);
            }

         } catch (JMSException e)
         {
            JMSExceptionMessage message = new JMSExceptionMessage(e);
            message.normalize(packet);
            sender.send(message);
         }
      }

      @Override
      public String toString()
      {
         return "ConnectionFactoryAdvisedPacketHandler[id=" + id + "]";
      }

   }

}
