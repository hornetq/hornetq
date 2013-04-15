/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
package org.hornetq.core.protocol.proton;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.impl.ClientConsumerImpl;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.QueueQueryResult;
import org.hornetq.core.server.ServerMessage;

import java.util.Map;

/**
 * A this is a wrapper around a HornetQ ServerConsumer for handling outgoing messages and incoming acks via a Proton Sender
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProtonConsumer implements ProtonDeliveryHandler
{
   private final static Symbol SELECTOR = Symbol.getSymbol("jms-selector");
   private final static Symbol COPY = Symbol.valueOf("copy");
   private final ProtonSession protonSession;
   private final HornetQServer server;
   private final Sender sender;
   private final ProtonRemotingConnection connection;
   private final ProtonProtocolManager protonProtocolManager;
   private long consumerID;
   private boolean closed = false;
   private long forcedDeliveryCount = 0;
   private boolean forcingDelivery = false;
   private boolean receivedForcedDelivery = true;

   public ProtonConsumer(ProtonRemotingConnection connection, Sender sender, ProtonSession protonSession, HornetQServer server,
                         ProtonProtocolManager protonProtocolManager)
   {
      this.connection = connection;
      this.sender = sender;
      this.protonSession = protonSession;
      this.server = server;
      this.protonProtocolManager = protonProtocolManager;
   }

   /*
   * start the session
   * */
   public void start() throws HornetQAMQPException
   {
      protonSession.getServerSession().start();

      //todo add flow control
      try
      {
         protonSession.getServerSession().receiveConsumerCredits(consumerID, -1);
      }
      catch (Exception e)
      {
         throw HornetQMessageBundle.BUNDLE.errorStartingConsumer(e.getMessage());
      }
   }

   /*
   * create the actual underlying HornetQ Server Consumer
   * */
   public void init() throws HornetQAMQPException
   {
      org.apache.qpid.proton.amqp.messaging.Source source = (org.apache.qpid.proton.amqp.messaging.Source) sender.getRemoteSource();

      SimpleString queue;

      consumerID = server.getStorageManager().generateUniqueID();

      SimpleString selector = null;
      Map filter = source.getFilter();
      if (filter != null)
      {
         DescribedType value = (DescribedType) filter.get(SELECTOR);
         if (value != null)
         {
            selector = new SimpleString(value.getDescribed().toString());
         }
      }

      if (source.getDynamic())
      {
         //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
         // will be deleted on closing of the session
         queue = new SimpleString(java.util.UUID.randomUUID().toString());
         try
         {
            protonSession.getServerSession().createQueue(queue, queue, null, true, false);
         }
         catch (Exception e)
         {
            throw HornetQMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
         }
         source.setAddress(queue.toString());
      } else
      {
         //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
         //be a queue bound to it so we nee to check this.
         String address = source.getAddress();
         if (address == null)
         {
            throw HornetQMessageBundle.BUNDLE.sourceAddressNotSet();
         }

         queue = new SimpleString(source.getAddress());
         QueueQueryResult queryResult;
         try
         {
            queryResult = protonSession.getServerSession().executeQueueQuery(new SimpleString(address));
         }
         catch (Exception e)
         {
            throw HornetQMessageBundle.BUNDLE.errorFindingTemporaryQueue(e.getMessage());
         }
         if (!queryResult.isExists())
         {
            throw HornetQMessageBundle.BUNDLE.sourceAddressDoesntExist();
         }
      }
      boolean browseOnly = source.getDistributionMode() != null && source.getDistributionMode().equals(COPY);
      try
      {
         protonSession.getServerSession().createConsumer(consumerID, queue, selector, browseOnly);
      }
      catch (Exception e)
      {
         throw HornetQMessageBundle.BUNDLE.errorCreatingHornetQConsumer(e.getMessage());
      }
   }

   /*
   * close the session
   * */
   public synchronized void close() throws HornetQAMQPException
   {
      closed = true;
      protonSession.removeConsumer(consumerID);
   }

   public long getConsumerID()
   {
      return consumerID;
   }

   /*
   * handle an out going message from HornetQ, send via the Proton Sender
   * */
   public synchronized int handleDelivery(ServerMessage message, int deliveryCount)
   {
      if (closed)
      {
         return 0;
      }
      if(message.containsProperty(ClientConsumerImpl.FORCED_DELIVERY_MESSAGE))
      {
         if(forcingDelivery)
         {
            sender.drained();
         }
         else
         {
            receivedForcedDelivery = true;
            forcingDelivery = false;
         }
         return 0;
      }
      //if we get here then a forced delivery has pushed some messages thru and we continue
      if(forcingDelivery)
      {
         forcingDelivery = false;
      }
      //presettle means we can ack the message on the proton side before we send it, i.e. for browsers
      boolean preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;
      //we only need a tag if we are going to ack later
      byte[] tag = preSettle ? new byte[0] : protonSession.getTag();
      //encode the message
      EncodedMessage encodedMessage = ProtonUtils.OUTBOUND.transform(message, deliveryCount);
      //now handle the delivery
      protonProtocolManager.handleDelivery(sender, tag, encodedMessage, message, connection, preSettle);

      return encodedMessage.getLength();
   }

   @Override
   /*
   * handle an incoming Ack from Proton, basically pass to HornetQ to handle
   * */
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      ServerMessage message = (ServerMessage) delivery.getContext();

      boolean preSettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;


      DeliveryState remoteState = delivery.getRemoteState();

      if (remoteState != null)
      {
         if (remoteState instanceof Accepted)
         {
            //we have to individual ack as we can't guarantee we will get the delivery updates (including acks) in order
            // from proton, a perf hit but a must
            try
            {
               protonSession.getServerSession().individualAcknowledge(consumerID, message.getMessageID());
            }
            catch (Exception e)
            {
               throw HornetQMessageBundle.BUNDLE.errorAcknowledgingMessage(message.getMessageID(), e.getMessage());
            }
         }
         else if (remoteState instanceof Released)
         {
            try
            {
               protonSession.getServerSession().individualCancel(consumerID, message.getMessageID(), false);
            }
            catch (Exception e)
            {
               throw HornetQMessageBundle.BUNDLE.errorCancellingMessage(message.getMessageID(), e.getMessage());
            }
         }
         else if (remoteState instanceof Rejected || remoteState instanceof Modified)
         {
            try
            {
               protonSession.getServerSession().individualCancel(consumerID, message.getMessageID(), true);
            }
            catch (Exception e)
            {
               throw HornetQMessageBundle.BUNDLE.errorCancellingMessage(message.getMessageID(), e.getMessage());
            }
         }

         synchronized (connection.getDeliveryLock())
         {
            delivery.settle();
         }
         //todo add tag caching
         if (!preSettle)
         {
            protonSession.replaceTag(delivery.getTag());
         }
         sender.offer(1);
      }
      else
      {
         //todo not sure if we need to do anything here
      }
   }

   /*
   * check the state of the consumer, i.e. are there any more messages. only really needed for browsers?
   * */
   public synchronized void checkState()
   {
      if (!forcingDelivery && receivedForcedDelivery)
      {
         try
         {
            forcingDelivery = true;
            receivedForcedDelivery = false;
            protonSession.getServerSession().forceConsumerDelivery(consumerID, forcedDeliveryCount++);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

   private String formatTag(byte[] tag)
   {
      StringBuffer sb = new StringBuffer();
      for (byte b : tag)
      {
         sb.append(b).append(":");
      }
      return sb.toString();
   }

   int x = 5;
}
