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

import org.jboss.jms.client.impl.ClientConnectionFactoryImpl;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.*;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UPDATECALLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONNECTION;
import org.jboss.messaging.util.ExceptionUtil;

import javax.jms.JMSException;

/**
 * A packet handler for creating connections. All ConnectionFactories will use the same instance of this
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ConnectionFactoryAdvisedPacketHandler implements
        PacketHandler
{
   private static final Logger log = Logger.getLogger(ConnectionFactoryAdvisedPacketHandler.class);
   MessagingServer messagingServer;


   public ConnectionFactoryAdvisedPacketHandler(MessagingServer messagingServer)
   {
      this.messagingServer = messagingServer;
   }/*
   * The advantage to use String as ID is that we can leverage Java 5 UUID to
   * generate these IDs. However theses IDs are 128 bite long and it increases
   * the size of a packet (compared to integer or long).
   *
   * By switching to Long, we could reduce the size of the packet and maybe
   * increase the performance (to check after some performance tests)
   */
   public String getID()
   {
      return ClientConnectionFactoryImpl.id;
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
                     request.getClientVMID(), request.getPrefetchSize(), request.getDupsOKBatchSize(),
                       request.getClientID(), request.getVersion());
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


   public CreateConnectionResponse createConnectionDelegate(String username,
                                                          String password,
                                                          String remotingSessionID,
                                                          String clientVMID, int prefetchSize, int dupsOKBatchSize,
                                                          String clientID,
                                                          byte versionToUse)
      throws JMSException
   {
      try
      {
         return
            createConnectionDelegateInternal(username, password,
                                             remotingSessionID, clientVMID,  prefetchSize, dupsOKBatchSize, clientID,
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
                                       String remotingSessionID, String clientVMID, int prefetchSize, int dupsOKBatchSize,
                                       String clientID,
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
      messagingServer.getConnectionManager().addConnectionFactoryCallback(getID(), VMID, remotingSessionID, sender);
   }

   public void removeSender(String VMID, String remotingSessionID,
         PacketSender sender) throws JMSException
   {
      log.debug("Removing PacketSender on ConnectionFactory");
      messagingServer.getConnectionManager().removeConnectionFactoryCallback(getID(), VMID, sender);
   }

}
