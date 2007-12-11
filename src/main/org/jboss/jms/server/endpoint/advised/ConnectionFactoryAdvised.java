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
package org.jboss.jms.server.endpoint.advised;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UPDATECALLBACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETCLIENTAOPSTACK;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETTOPOLOGY;

import javax.jms.JMSException;

import org.jboss.jms.delegate.ConnectionFactoryEndpoint;
import org.jboss.jms.delegate.CreateConnectionResult;
import org.jboss.jms.delegate.TopologyResult;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.server.endpoint.ConnectionFactoryInternalEndpoint;
import org.jboss.jms.server.endpoint.ServerConnectionFactoryEndpoint;
import org.jboss.messaging.core.remoting.Assert;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;
import org.jboss.messaging.core.remoting.wireformat.GetClientAOPStackResponse;
import org.jboss.messaging.core.remoting.wireformat.GetTopologyResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.UpdateCallbackMessage;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>1.5</tt>
 *
 * ConnectionFactoryAdvised.java,v 1.3 2006/03/01 22:56:51 ovidiu Exp
 */
public class ConnectionFactoryAdvised extends AdvisedSupport
   implements ConnectionFactoryInternalEndpoint
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   protected ConnectionFactoryEndpoint endpoint;

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactoryAdvised()
   {
   }

   public ConnectionFactoryAdvised(ConnectionFactoryEndpoint endpoint)
   {
      this.endpoint = endpoint;
   }

   // ConnectionFactoryEndpoint implementation -----------------------------------------------------

   public CreateConnectionResult createConnectionDelegate(String username,
                                                          String password,
                                                          int failedNodeId)
      throws JMSException
   {
      return endpoint
            .createConnectionDelegate(username, password, failedNodeId);
   }

   public byte[] getClientAOPStack() throws JMSException
   {
      return endpoint.getClientAOPStack();
   }

   public void addSender(String vmID, String remotingSessionID,
         PacketSender sender) throws JMSException
   {
      ((ServerConnectionFactoryEndpoint)endpoint).addSender(vmID,  remotingSessionID,
            sender);
   }
   
   public void removeSender(String vmID, String remotingSessionID,
         PacketSender sender) throws JMSException
   {
      ((ServerConnectionFactoryEndpoint)endpoint).removeSender(vmID,  remotingSessionID,
          sender);
   }

   public TopologyResult getTopology() throws JMSException
   {
      return endpoint.getTopology();
   }

   // ConnectionFactoryInternalEndpoint implementation ---------------------------------------------
   public CreateConnectionResult
      createConnectionDelegate(String username,
                               String password,
                               int failedNodeID,
                               String remotingSessionID,
                               String clientVMID,
                               byte versionToUse)
      throws JMSException
   {
      return ((ServerConnectionFactoryEndpoint)endpoint).
         createConnectionDelegate(username, password, failedNodeID,
                                  remotingSessionID, clientVMID,
                                  versionToUse);
   }

   // AdvisedSupport override ----------------------------------------------------------------------

   public Object getEndpoint()
   {
      return endpoint;
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConnectionFactoryAdvised->" + endpoint;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   public final class ConnectionFactoryAdvisedPacketHandler implements
   PacketHandler
   {
      private final String id;

      public ConnectionFactoryAdvisedPacketHandler(String id)
      {
         Assert.assertValidID(id);
         
         this.id = id;
      }
      
      public String getID()
      {
         return id;
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
               CreateConnectionResult del = createConnectionDelegate(request
                     .getUsername(), request.getPassword(), request
                     .getFailedNodeID(), request.getRemotingSessionID(),
                     request.getClientVMID(), request.getVersion());

               response = new CreateConnectionResponse(del.getDelegate()
                     .getID(), del.getDelegate().getServerID());
            } else if (type == REQ_GETCLIENTAOPSTACK)
            {
               byte[] stack = getClientAOPStack();

               response = new GetClientAOPStackResponse(stack);
            } else if (type == REQ_GETTOPOLOGY)
            {
               TopologyResult topology = getTopology();

               response = new GetTopologyResponse(topology);
            } else if (type == MSG_UPDATECALLBACK)
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
