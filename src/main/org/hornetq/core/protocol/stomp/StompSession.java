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
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.protocol.stomp.Stomp.Headers;
import org.hornetq.core.server.QueueQueryResult;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.remoting.ReadyListener;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.UUIDGenerator;

/**
 * A StompSession
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
class StompSession implements SessionCallback
{
   private static final Logger log = Logger.getLogger(StompSession.class);

   private final StompProtocolManager manager;

   private final StompConnection connection;

   private ServerSession session;

   private final OperationContext sessionContext;

   private final Map<Long, StompSubscription> subscriptions = new ConcurrentHashMap<Long, StompSubscription>();

   // key = message ID, value = consumer ID
   private final Map<Long, Long> messagesToAck = new ConcurrentHashMap<Long, Long>();

   private volatile boolean noLocal = false;

   StompSession(final StompConnection connection, final StompProtocolManager manager, OperationContext sessionContext)
   {
      this.connection = connection;
      this.manager = manager;
      this.sessionContext = sessionContext;
   }

   void setServerSession(ServerSession session)
   {
      this.session = session;
   }

   public ServerSession getSession()
   {
      return session;
   }

   public void sendProducerCreditsMessage(int credits, SimpleString address)
   {
   }

   public int sendMessage(ServerMessage serverMessage, long consumerID, int deliveryCount)
   {
      try
      {
         StompSubscription subscription = subscriptions.get(consumerID);

         Map<String, Object> headers = new HashMap<String, Object>();
         headers.put(Stomp.Headers.Message.DESTINATION, serverMessage.getAddress().toString());
         if (subscription.getID() != null)
         {
            headers.put(Stomp.Headers.Message.SUBSCRIPTION, subscription.getID());
         }
         HornetQBuffer buffer = serverMessage.getBodyBuffer();

         int bodyPos = serverMessage.getEndOfBodyPosition() == -1 ? buffer.writerIndex()
                                                                 : serverMessage.getEndOfBodyPosition();
         int size = bodyPos - buffer.readerIndex();
         buffer.readerIndex(MessageImpl.BUFFER_HEADER_SPACE + DataConstants.SIZE_INT);
         byte[] data = new byte[size];
         if (serverMessage.containsProperty(Stomp.Headers.CONTENT_LENGTH) || serverMessage.getType() == Message.BYTES_TYPE)
         {
            headers.put(Headers.CONTENT_LENGTH, data.length);
            buffer.readBytes(data);
         }
         else
         {
            SimpleString text = buffer.readNullableSimpleString();
            if (text != null)
            {
               data = text.toString().getBytes("UTF-8");
            }
            else
            {
               data = new byte[0];
            }
         }
         serverMessage.getBodyBuffer().resetReaderIndex();
         StompFrame frame = new StompFrame(Stomp.Responses.MESSAGE, headers, data);
         StompUtils.copyStandardHeadersFromMessageToFrame(serverMessage, frame, deliveryCount);

         if (subscription.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.AUTO))
         {
            session.acknowledge(consumerID, serverMessage.getMessageID());
            session.commit();
         }
         else
         {
            messagesToAck.put(serverMessage.getMessageID(), consumerID);
         }

         // Must send AFTER adding to messagesToAck - or could get acked from client BEFORE it's been added!
         manager.send(connection, frame);
         int length = frame.getEncodedSize();

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

   public int sendLargeMessage(ServerMessage msg, long consumerID, long bodySize, int deliveryCount)
   {
      return 0;
   }

   public void closed()
   {
   }
   
   public void addReadyListener(final ReadyListener listener)
   {
      connection.getTransportConnection().addReadyListener(listener);      
   }

   public void removeReadyListener(final ReadyListener listener)
   {
      connection.getTransportConnection().removeReadyListener(listener);
   }

   public void acknowledge(String messageID) throws Exception
   {
      long id = Long.parseLong(messageID);
      long consumerID = messagesToAck.remove(id);
      session.acknowledge(consumerID, id);
      session.commit();
   }

   public void addSubscription(long consumerID,
                               String subscriptionID,
                               String clientID,
                               String durableSubscriptionName,
                               String destination,
                               String selector,
                               String ack) throws Exception
   {
      SimpleString queue = SimpleString.toSimpleString(destination);
      if (destination.startsWith("jms.topic"))
      {
         // subscribes to a topic
         if (durableSubscriptionName != null)
         {
            if (clientID == null)
            {
               throw new IllegalStateException("Cannot create a subscriber on the durable subscription if the client-id of the connection is not set");
            }
            queue = SimpleString.toSimpleString(clientID + "." + durableSubscriptionName);
            QueueQueryResult query = session.executeQueueQuery(queue);
            if (!query.isExists())
            {
               session.createQueue(SimpleString.toSimpleString(destination), queue, null, false, true);
            }
            else
            {
               // Already exists
               if (query.getConsumerCount() > 0)
               {
                  throw new IllegalStateException("Cannot create a subscriber on the durable subscription since it already has a subscriber: " + queue);
               }
            }
         }
         else
         {
            queue = UUIDGenerator.getInstance().generateSimpleStringUUID();
            session.createQueue(SimpleString.toSimpleString(destination), queue, null, true, false);
         }
      }
      session.createConsumer(consumerID, queue, SimpleString.toSimpleString(selector), false);
      session.receiveConsumerCredits(consumerID, -1);
      StompSubscription subscription = new StompSubscription(subscriptionID, ack);
      subscriptions.put(consumerID, subscription);
      // FIXME not very smart: since we can't start the consumer, we start the session
      // every time to start the new consumer (and all previous consumers...)
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

   public OperationContext getContext()
   {
      return sessionContext;
   }

   public boolean isNoLocal()
   {
      return noLocal;
   }

   public void setNoLocal(boolean noLocal)
   {
      this.noLocal = noLocal;
   }
}