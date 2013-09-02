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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.Inflater;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.QueueQueryResult;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.protocol.SessionCallback;
import org.hornetq.spi.core.remoting.ReadyListener;
import org.hornetq.utils.ConfigurationHelper;
import org.hornetq.utils.UUIDGenerator;

/**
 * A StompSession
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class StompSession implements SessionCallback
{
   private final StompProtocolManager manager;

   private final StompConnection connection;

   private ServerSession session;

   private final OperationContext sessionContext;

   private final Map<Long, StompSubscription> subscriptions = new ConcurrentHashMap<Long, StompSubscription>();

   // key = message ID, value = consumer ID
   private final Map<Long, Pair<Long, Integer>> messagesToAck = new ConcurrentHashMap<Long, Pair<Long, Integer>>();

   private volatile boolean noLocal = false;

   private final int consumerCredits;

   StompSession(final StompConnection connection, final StompProtocolManager manager, OperationContext sessionContext)
   {
      this.connection = connection;
      this.manager = manager;
      this.sessionContext = sessionContext;
      this.consumerCredits = ConfigurationHelper.getIntProperty(TransportConstants.STOMP_CONSUMERS_CREDIT,
                                                               TransportConstants.STOMP_DEFAULT_CONSUMERS_CREDIT,
                                                               connection.getAcceptorUsed().getConfiguration());
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

   public void sendProducerCreditsFailMessage(int credits, SimpleString address)
   {
   }

   public int sendMessage(ServerMessage serverMessage, long consumerID, int deliveryCount)
   {
      LargeServerMessageImpl largeMessage = null;
      ServerMessage newServerMessage = serverMessage;
      try
      {
         StompSubscription subscription = subscriptions.get(consumerID);
         StompFrame frame = null;
         if (serverMessage.isLargeMessage())
         {
            newServerMessage = serverMessage.copy();

            largeMessage = (LargeServerMessageImpl)serverMessage;
            BodyEncoder encoder = largeMessage.getBodyEncoder();
            encoder.open();
            int bodySize = (int)encoder.getLargeBodySize();

            //large message doesn't have a body.
            ((ServerMessageImpl)newServerMessage).createBody(bodySize);
            encoder.encode(newServerMessage.getBodyBuffer(), bodySize);
            encoder.close();
         }

         if (serverMessage.getBooleanProperty(Message.HDR_LARGE_COMPRESSED))
         {
            //decompress
            HornetQBuffer qbuff = newServerMessage.getBodyBuffer();
            int bytesToRead = qbuff.writerIndex() - MessageImpl.BODY_OFFSET;
            Inflater inflater = new Inflater();
            inflater.setInput(qbuff.readBytes(bytesToRead).toByteBuffer().array());
            
            
            //get the real size of large message
            long sizeBody = newServerMessage.getLongProperty(Message.HDR_LARGE_BODY_SIZE);

            byte[] data = new byte[(int)sizeBody];
            inflater.inflate(data);
            inflater.end();
            qbuff.resetReaderIndex();
            qbuff.resetWriterIndex();
            qbuff.writeBytes(data);
         }

         frame = connection.createStompMessage(newServerMessage, subscription, deliveryCount);

         int length = frame.getEncodedSize();

         if (subscription.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.AUTO))
         {
            if (manager.send(connection, frame))
            {
               //we ack and commit only if the send is successful
               session.acknowledge(consumerID, newServerMessage.getMessageID());
               session.commit();
            }
         }
         else
         {
            messagesToAck.put(newServerMessage.getMessageID(), new Pair<Long, Integer>(consumerID, length));
            // Must send AFTER adding to messagesToAck - or could get acked from client BEFORE it's been added!
            manager.send(connection, frame);
         }

         return length;
      }
      catch (Exception e)
      {
         return 0;
      }
      finally
      {
         if (largeMessage != null)
         {
            largeMessage.releaseResources();
            largeMessage = null;
         }
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

   public void acknowledge(String messageID, String subscriptionID) throws Exception
   {
      long id = Long.parseLong(messageID);
      Pair<Long, Integer> pair = messagesToAck.remove(id);

      if (pair == null)
      {
         throw new HornetQStompException("failed to ack because no message with id: " + id);
      }

      long consumerID = pair.getA();
      int credits = pair.getB();

      StompSubscription sub = subscriptions.get(consumerID);

      if (subscriptionID != null)
      {
         if (!sub.getID().equals(subscriptionID))
         {
            throw new HornetQStompException("subscription id " + subscriptionID + " does not match " + sub.getID());
         }
      }

      if (this.consumerCredits != -1)
      {
         session.receiveConsumerCredits(consumerID, credits);
      }

      if (sub.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.CLIENT_INDIVIDUAL))
      {
         session.individualAcknowledge(consumerID, id);
      }
      else
      {
         session.acknowledge(consumerID, id);
      }

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
               session.createQueue(SimpleString.toSimpleString(destination), queue, SimpleString.toSimpleString(selector), false, true);
            }
         }
         else
         {
            queue = UUIDGenerator.getInstance().generateSimpleStringUUID();
            session.createQueue(SimpleString.toSimpleString(destination), queue, SimpleString.toSimpleString(selector), true, false);
         }
         ((ServerSessionImpl)session).createConsumer(consumerID, queue, null, false, false);
      } 
      else 
      {
          ((ServerSessionImpl)session).createConsumer(consumerID, queue, SimpleString.toSimpleString(selector), false, false); 
      }

      StompSubscription subscription = new StompSubscription(subscriptionID, ack);
      subscriptions.put(consumerID, subscription);

      if (subscription.getAck().equals(Stomp.Headers.Subscribe.AckModeValues.AUTO))
      {
         session.receiveConsumerCredits(consumerID, -1);
      }
      else
      {
         session.receiveConsumerCredits(consumerID, consumerCredits);
      }

      session.start();
   }

   public boolean unsubscribe(String id, String durableSubscriptionName) throws Exception
   {
      Iterator<Entry<Long, StompSubscription>> iterator = subscriptions.entrySet().iterator();
      while (iterator.hasNext())
      {
         Map.Entry<Long, StompSubscription> entry = iterator.next();
         long consumerID = entry.getKey();
         StompSubscription sub = entry.getValue();
         if (id != null && id.equals(sub.getID()))
         {
            iterator.remove();
            session.closeConsumer(consumerID);
            SimpleString queueName;
            if (durableSubscriptionName != null && durableSubscriptionName.trim().length() != 0) {
                queueName = SimpleString.toSimpleString(id + "." + durableSubscriptionName);
            } else {
                queueName = SimpleString.toSimpleString(id);
            }
            QueueQueryResult query = session.executeQueueQuery(queueName);
            if (query.isExists()) {
               session.deleteQueue(queueName);
            }
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
         Map.Entry<Long, StompSubscription> entry = iterator.next();
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

   public void sendInternal(ServerMessageImpl message, boolean direct)
         throws Exception
   {
      session.send(message, direct);
   }

   public void sendInternalLarge(ServerMessageImpl message, boolean direct)
         throws Exception
   {
      int headerSize = message.getHeadersAndPropertiesEncodeSize();
      if (headerSize >= connection.getMinLargeMessageSize())
      {
         throw new Exception("Message header too big, increase minLargeMessageSize please.");
      }

      StorageManager storageManager = ((ServerSessionImpl)session).getStorageManager();
      long id = storageManager.generateUniqueID();
      LargeServerMessage largeMessage = storageManager.createLargeMessage(id, message);
      
      byte[] bytes = new byte[message.getBodyBuffer().writerIndex() - MessageImpl.BODY_OFFSET];
      message.getBodyBuffer().readBytes(bytes);
      
      largeMessage.addBytes(bytes);
      
      largeMessage.releaseResources();

      largeMessage.putLongProperty(Message.HDR_LARGE_BODY_SIZE, bytes.length);

      session.send(largeMessage, direct);

      largeMessage = null;
   }

}
