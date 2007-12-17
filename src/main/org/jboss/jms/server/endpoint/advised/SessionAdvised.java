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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ACKDELIVERIES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ADDTEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERIES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CANCELDELIVERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELETETEMPORARYDESTINATION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_RECOVERDELIVERIES;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDMESSAGE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UNSUBSCRIBE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_ACKDELIVERY;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CLOSING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEBROWSER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONSUMER;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEDESTINATION;

import java.util.List;

import javax.jms.JMSException;

import org.jboss.jms.client.delegate.ClientBrowserDelegate;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.delegate.Ack;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.Cancel;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.DefaultAck;
import org.jboss.jms.delegate.SessionEndpoint;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.server.endpoint.ServerSessionEndpoint;
import org.jboss.jms.server.endpoint.SessionInternalEndpoint;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryRequest;
import org.jboss.messaging.core.remoting.wireformat.AcknowledgeDeliveryResponse;
import org.jboss.messaging.core.remoting.wireformat.AddTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.CancelDeliveryMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateDestinationResponse;
import org.jboss.messaging.core.remoting.wireformat.DeleteTemporaryDestinationMessage;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.RecoverDeliveriesMessage;
import org.jboss.messaging.core.remoting.wireformat.SendMessage;
import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;
import org.jboss.messaging.newcore.Message;

/**
 * The server-side advised instance corresponding to a Session. It is bound to the AOP
 * Dispatcher's map.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SessionAdvised extends AdvisedSupport implements SessionInternalEndpoint
{
   // Constants -----------------------------------------------------
	
   // Attributes ----------------------------------------------------

   protected SessionEndpoint endpoint;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAdvised(SessionEndpoint endpoint)
   {
      this.endpoint = endpoint;
   }

   // SessionEndpoint implementation --------------------------------

   public void close() throws JMSException
   {
      endpoint.close();
   }

   public long closing(long sequence) throws JMSException
   {
      return endpoint.closing(sequence);
   }

   public void send(Message msg, boolean checkForDuplicates) throws JMSException
   {
      throw new IllegalStateException("Invocation should not be handle here");
   }
   
   public void send(Message msg, boolean checkForDuplicates, long seq) throws JMSException
   {
      ((ServerSessionEndpoint)endpoint).send(msg, checkForDuplicates, seq);
   }
   
   public ConsumerDelegate createConsumerDelegate(JBossDestination destination, String selector,
                                                  boolean noLocal, String subscriptionName,
                                                  boolean connectionConsumer, boolean autoFlowControl) throws JMSException
   {
      return endpoint.createConsumerDelegate(destination, selector, noLocal, subscriptionName,
                                             connectionConsumer, autoFlowControl);
   }
   
   public BrowserDelegate createBrowserDelegate(JBossDestination queue, String messageSelector) throws JMSException                                                 
   {
      return endpoint.createBrowserDelegate(queue, messageSelector);
   }

   public JBossQueue createQueue(String queueName) throws JMSException
   {
      return endpoint.createQueue(queueName);
   }

   public JBossTopic createTopic(String topicName) throws JMSException
   {
      return endpoint.createTopic(topicName);
   }

   public void acknowledgeDeliveries(List acks) throws JMSException
   {
      endpoint.acknowledgeDeliveries(acks);
   }
   
   public boolean acknowledgeDelivery(Ack ack) throws JMSException
   {
      return endpoint.acknowledgeDelivery(ack);
   }

   public void addTemporaryDestination(JBossDestination destination) throws JMSException
   {
      endpoint.addTemporaryDestination(destination);
   }

   public void deleteTemporaryDestination(JBossDestination destination) throws JMSException
   {
      endpoint.deleteTemporaryDestination(destination);
   }

   public void unsubscribe(String subscriptionName) throws JMSException
   {
      endpoint.unsubscribe(subscriptionName);
   }
   
   public void cancelDeliveries(List ackInfos) throws JMSException
   {
      endpoint.cancelDeliveries(ackInfos);
   }

   public void cancelDelivery(Cancel cancel) throws JMSException
   {
      endpoint.cancelDelivery(cancel);
   }
   
   public void recoverDeliveries(List ackInfos, String oldSessionID) throws JMSException
   {
      endpoint.recoverDeliveries(ackInfos, oldSessionID);
   }

   // AdvisedSupport overrides --------------------------------------

   public Object getEndpoint()
   {
      return endpoint;
   }

   public String toString()
   {
      return "SessionAdvised->" + endpoint;
   }

   // Public --------------------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------

   public class SessionAdvisedPacketHandler implements PacketHandler {

      private String id;

      public SessionAdvisedPacketHandler(String id)
      {
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
            if (type == MSG_SENDMESSAGE)
            {
               SendMessage message = (SendMessage) packet;
           
               long sequence = message.getSequence(); 
               send(message.getMessage(), message.checkForDuplicates(), sequence);

               // a response is required only if seq == -1 -> reliable message or strict TCK
               if (sequence == -1)
               {               
                  response = new NullPacket();
               }
               
            } else if (type == REQ_CREATECONSUMER)
            {
               CreateConsumerRequest request = (CreateConsumerRequest) packet;
               ClientConsumerDelegate consumer = (ClientConsumerDelegate) createConsumerDelegate(
                     request.getDestination(), request.getSelector(), request
                           .isNoLocal(), request.getSubscriptionName(), request
                           .isConnectionConsumer(), request.isAutoFlowControl());

               response = new CreateConsumerResponse(consumer.getID(), consumer
                     .getBufferSize(), consumer.getMaxDeliveries(), consumer
                     .getRedeliveryDelay());
            } else if (type == REQ_CREATEDESTINATION)
            {
               CreateDestinationRequest request = (CreateDestinationRequest) packet;
               JBossDestination destination;
               if (request.isQueue())
               {
                  destination = createQueue(request.getName());
               } else
               {
                  destination = createTopic(request.getName());
               }

               response = new CreateDestinationResponse(destination);
            } else if (type == REQ_CREATEBROWSER)
            {
               CreateBrowserRequest request = (CreateBrowserRequest) packet;
               ClientBrowserDelegate browser = (ClientBrowserDelegate) createBrowserDelegate(
                     request.getDestination(), request.getSelector());

               response = new CreateBrowserResponse(browser.getID());
            } else if (type == REQ_ACKDELIVERY)
            {
               AcknowledgeDeliveryRequest request = (AcknowledgeDeliveryRequest) packet;
               boolean acknowledged = acknowledgeDelivery(new DefaultAck(
                     request.getDeliveryID()));

               response = new AcknowledgeDeliveryResponse(acknowledged);
            } else if (type == MSG_ACKDELIVERIES)
            {
               AcknowledgeDeliveriesMessage message = (AcknowledgeDeliveriesMessage) packet;
               acknowledgeDeliveries(message.getAcks());

               response = new NullPacket();
            } else if (type == MSG_RECOVERDELIVERIES)
            {
               RecoverDeliveriesMessage message = (RecoverDeliveriesMessage) packet;
               recoverDeliveries(message.getDeliveries(), message
                     .getSessionID());

               response = new NullPacket();
            } else if (type == MSG_CANCELDELIVERY)
            {
               CancelDeliveryMessage message = (CancelDeliveryMessage) packet;
               cancelDelivery(message.getCancel());

               response = new NullPacket();
            } else if (type == MSG_CANCELDELIVERIES)
            {
               CancelDeliveriesMessage message = (CancelDeliveriesMessage) packet;
               cancelDeliveries(message.getCancels());

               response = new NullPacket();
            } else if (type == REQ_CLOSING)
            {
               ClosingRequest request = (ClosingRequest) packet;
               long id = closing(request.getSequence());

               response = new ClosingResponse(id);
            } else if (type == MSG_CLOSE)
            {
               close();

               response = new NullPacket();
            } else if (type == MSG_UNSUBSCRIBE)
            {
               UnsubscribeMessage message = (UnsubscribeMessage) packet;
               unsubscribe(message.getSubscriptionName());

               response = new NullPacket();
            } else if (type == MSG_ADDTEMPORARYDESTINATION)
            {
               AddTemporaryDestinationMessage message = (AddTemporaryDestinationMessage) packet;
               addTemporaryDestination(message.getDestination());

               response = new NullPacket();
            } else if (type == MSG_DELETETEMPORARYDESTINATION)
            {
               DeleteTemporaryDestinationMessage message = (DeleteTemporaryDestinationMessage) packet;
               deleteTemporaryDestination(message.getDestination());

               response = new NullPacket();
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
         return "SessionAdvisedPacketHandler[id=" + id + "]";
      }
   }
}
