/*
 * Copyright 2010 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.protocol.stomp;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.protocol.stomp.Stomp.Headers;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;

/**
 * A StompSession
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
class StompSession implements SessionCallback
{
   private final StompProtocolManager manager;

   private final StompConnection connection;

   private ServerSession session;

   private final Map<Long, StompSubscription> subscriptions = new HashMap<Long, StompSubscription>();

   // key = message ID, value = consumer ID
   private final Map<Long, Long> messagesToAck = new HashMap<Long, Long>();

   StompSession(final StompConnection connection, final StompProtocolManager manager)
   {
      this.connection = connection;
      this.manager = manager;
   }

   void setServerSession(ServerSession session)
   {
      this.session = session;
   }

   public ServerSession getSession()
   {
      return session;
   }

   public void sendProducerCreditsMessage(int credits, SimpleString address, int offset)
   {
   }

   public int sendMessage(ServerMessage serverMessage, long consumerID, int deliveryCount)
   {
      try
      {
         StompSubscription subscription = subscriptions.get(consumerID);

         Map<String, Object> headers = new HashMap<String, Object>();
         headers.put(Stomp.Headers.Message.DESTINATION, StompUtils.toStompDestination(serverMessage.getAddress()
                                                                                                   .toString()));
         if (subscription.getID() != null)
         {
            headers.put(Stomp.Headers.Message.SUBSCRIPTION, subscription.getID());
         }
         byte[] data = new byte[] {};
         if (serverMessage.getType() == Message.TEXT_TYPE)
         {
            SimpleString text = serverMessage.getBodyBuffer().readNullableSimpleString();
            if (text != null)
            {
               data = text.toString().getBytes();
            }
         }
         else
         {
            HornetQBuffer buffer = serverMessage.getBodyBuffer();
            buffer.readerIndex(MessageImpl.BUFFER_HEADER_SPACE);
            int size = serverMessage.getEndOfBodyPosition() - buffer.readerIndex();
            data = new byte[size];
            buffer.readBytes(data);
            headers.put(Headers.CONTENT_LENGTH, data.length);
         }
         
         StompFrame frame = new StompFrame(Stomp.Responses.MESSAGE, headers, data);
         StompUtils.copyStandardHeadersFromMessageToFrame(serverMessage, frame, deliveryCount);
         
         int length = manager.send(connection, frame);

         if (subscription.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.AUTO))
         {
            session.acknowledge(consumerID, serverMessage.getMessageID());
            session.commit();
         }
         else
         {
            messagesToAck.put(serverMessage.getMessageID(), consumerID);
         }
         return length;

      }
      catch (Exception e)
      {
         e.printStackTrace();
         return 0;
      }

   }

   public int sendLargeMessageContinuation(long consumerID, byte[] body, boolean continues, boolean requiresResponse)
   {
      return 0;
   }

   public int sendLargeMessage(long consumerID, byte[] headerBuffer, long bodySize, int deliveryCount)
   {
      return 0;
   }

   public void closed()
   {
   }

   public void acknowledge(String messageID) throws Exception
   {
      long id = Long.parseLong(messageID);
      long consumerID = messagesToAck.remove(id);
      session.acknowledge(consumerID, id);
      session.commit();
   }

   public void addSubscription(long consumerID, String subscriptionID, String destination, String selector, String ack) throws Exception
   {
      String queue = StompUtils.toHornetQAddress(destination);
         session.createConsumer(consumerID,
                                SimpleString.toSimpleString(queue),
                                SimpleString.toSimpleString(selector),
                                false);
         session.receiveConsumerCredits(consumerID, -1);
         StompSubscription subscription = new StompSubscription(subscriptionID, destination, ack);
         subscriptions.put(consumerID, subscription);
         // FIXME not very smart: since we can't start the consumer, we start the session
         // everytime to start the new consumer (and all previous consumers...)
         session.start();
   }

   public boolean unsubscribe(String id) throws Exception
   {
      Iterator<Entry<Long, StompSubscription>> iterator = subscriptions.entrySet().iterator();
      while (iterator.hasNext())
      {
         Map.Entry<Long, StompSubscription> entry = (Map.Entry<Long, StompSubscription>)iterator.next();
         long consumerID = entry.getKey();
         StompSubscription sub = entry.getValue();
         if (id != null && id.equals(sub.getID()))
         {
            iterator.remove();
            session.closeConsumer(consumerID);
            return true;
         }
      }
      return false;
   }

   boolean containsSubscription(String subscriptionID)
   {     
      Iterator<Entry<Long, StompSubscription>> iterator = subscriptions.entrySet().iterator();
      while (iterator.hasNext())
      {
         Map.Entry<Long, StompSubscription> entry = (Map.Entry<Long, StompSubscription>)iterator.next();
         StompSubscription sub = entry.getValue();
         if (sub.getID().equals(subscriptionID))
         {
            return true;
         }
      }
      return false;
   }

   public RemotingConnection getConnection()
   {
      return connection;
   }
}