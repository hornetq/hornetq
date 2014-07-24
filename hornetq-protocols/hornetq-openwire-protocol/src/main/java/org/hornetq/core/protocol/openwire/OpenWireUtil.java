/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.core.protocol.openwire;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQPropertyConversionException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.openwire.amq.AMQConsumer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;

public class OpenWireUtil
{
   private static final String AMQ_PREFIX = "__HDR_";

   private static final String AMQ_MSG_ARRIVAL = AMQ_PREFIX + "ARRIVAL";
   private static final String AMQ_MSG_BROKER_IN_TIME = AMQ_PREFIX + "BROKER_IN_TIME";
//   private static final String AMQ_MSG_BROKER_OUT_TIME = AMQ_PREFIX + "BROKER_OUT_TIME";
   private static final String AMQ_MSG_BROKER_PATH = AMQ_PREFIX + "BROKER_PATH";
   private static final String AMQ_MSG_CLUSTER = AMQ_PREFIX + "CLUSTER";
   private static final String AMQ_MSG_COMMAND_ID = AMQ_PREFIX + "COMMAND_ID";
   private static final String AMQ_MSG_DATASTRUCTURE = AMQ_PREFIX + "DATASTRUCTURE";
   private static final String AMQ_MSG_DESTINATION = AMQ_PREFIX + "DESTINATION";
   private static final String AMQ_MSG_GROUP_ID = AMQ_PREFIX + "GROUP_ID";
   private static final String AMQ_MSG_GROUP_SEQUENCE = AMQ_PREFIX + "GROUP_SEQUENCE";
   private static final String AMQ_MSG_MESSAGE_ID = AMQ_PREFIX + "MESSAGE_ID";
   private static final String AMQ_MSG_ORIG_DESTINATION = AMQ_PREFIX + "ORIG_DESTINATION";
   private static final String AMQ_MSG_ORIG_TXID = AMQ_PREFIX + "ORIG_TXID";
   private static final String AMQ_MSG_PRODUCER_ID = AMQ_PREFIX + "PRODUCER_ID";
   private static final String AMQ_MSG_MARSHALL_PROP = AMQ_PREFIX + "MARSHALL_PROP";
   private static final String AMQ_MSG_REDELIVER_COUNTER = AMQ_PREFIX + "REDELIVER_COUNTER";
   private static final String AMQ_MSG_REPLY_TO = AMQ_PREFIX + "REPLY_TO";

   private static final String AMQ_MSG_CONSUMER_ID = AMQ_PREFIX + "CONSUMER_ID";
   private static final String AMQ_MSG_TX_ID = AMQ_PREFIX + "TX_ID";
   private static final String AMQ_MSG_USER_ID = AMQ_PREFIX + "USER_ID";

   private static final String AMQ_MSG_COMPRESSED = AMQ_PREFIX + "COMPRESSED";
   private static final String AMQ_MSG_DROPPABLE = AMQ_PREFIX + "DROPPABLE";

   public static HornetQBuffer toHornetQBuffer(ByteSequence bytes)
   {
      HornetQBuffer buffer = HornetQBuffers.fixedBuffer(bytes.length);

      buffer.writeBytes(bytes.data, bytes.offset, bytes.length);
      return buffer;
   }

   //convert an ActiveMQ message to coreMessage
   public static void toCoreMessage(ServerMessageImpl coreMessage, Message messageSend, WireFormat marshaller) throws IOException
   {
      String type = messageSend.getType();
      if (type != null)
      {
         coreMessage.putStringProperty(new SimpleString("JMSType"), new SimpleString(type));
      }
      coreMessage.setDurable(messageSend.isPersistent());
      coreMessage.setExpiration(messageSend.getExpiration());
      coreMessage.setPriority(messageSend.getPriority());
      coreMessage.setTimestamp(messageSend.getTimestamp());
      coreMessage.setType(toCoreType(messageSend.getDataStructureType()));

      ByteSequence contents = messageSend.getContent();
      if (contents != null)
      {
         HornetQBuffer body = coreMessage.getBodyBuffer();
         body.writeBytes(contents.data, contents.offset, contents.length);
      }
      //amq specific
      coreMessage.putLongProperty(AMQ_MSG_ARRIVAL, messageSend.getArrival());
      coreMessage.putLongProperty(AMQ_MSG_BROKER_IN_TIME, messageSend.getBrokerInTime());
      BrokerId[] brokers = messageSend.getBrokerPath();
      if (brokers != null)
      {
         StringBuilder builder = new StringBuilder();
         for (int i = 0; i < brokers.length; i++)
         {
            builder.append(brokers[i].getValue());
            if (i != (brokers.length - 1))
            {
               builder.append(","); //is this separator safe?
            }
         }
         coreMessage.putStringProperty(AMQ_MSG_BROKER_PATH, builder.toString());
      }
      BrokerId[] cluster = messageSend.getCluster();
      if (cluster != null)
      {
         StringBuilder builder = new StringBuilder();
         for (int i = 0; i < cluster.length; i++)
         {
            builder.append(cluster[i].getValue());
            if (i != (cluster.length - 1))
            {
               builder.append(","); //is this separator safe?
            }
         }
         coreMessage.putStringProperty(AMQ_MSG_CLUSTER, builder.toString());
      }

      coreMessage.putIntProperty(AMQ_MSG_COMMAND_ID, messageSend.getCommandId());
      String corrId = messageSend.getCorrelationId();
      if (corrId != null)
      {
         coreMessage.putStringProperty("JMSCorrelationID", corrId);
      }
      DataStructure ds = messageSend.getDataStructure();
      if (ds != null)
      {
         ByteSequence dsBytes = marshaller.marshal(ds);
         dsBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_DATASTRUCTURE, dsBytes.data);
      }
      ActiveMQDestination dest = messageSend.getDestination();
      ByteSequence destBytes = marshaller.marshal(dest);
      destBytes.compact();
      coreMessage.putBytesProperty(AMQ_MSG_DESTINATION, destBytes.data);
      String groupId = messageSend.getGroupID();
      if (groupId != null)
      {
         coreMessage.putStringProperty(AMQ_MSG_GROUP_ID, groupId);
      }
      coreMessage.putIntProperty(AMQ_MSG_GROUP_SEQUENCE, messageSend.getGroupSequence());

      MessageId messageId = messageSend.getMessageId();

      ByteSequence midBytes = marshaller.marshal(messageId);
      midBytes.compact();
      coreMessage.putBytesProperty(AMQ_MSG_MESSAGE_ID, midBytes.data);

      ActiveMQDestination origDest = messageSend.getOriginalDestination();
      if (origDest != null)
      {
         ByteSequence origDestBytes = marshaller.marshal(origDest);
         origDestBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_ORIG_DESTINATION, origDestBytes.data);
      }
      TransactionId origTxId = messageSend.getOriginalTransactionId();
      if (origTxId != null)
      {
         ByteSequence origTxBytes = marshaller.marshal(origTxId);
         origTxBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_ORIG_TXID, origTxBytes.data);
      }
      ProducerId producerId = messageSend.getProducerId();
      if (producerId != null)
      {
         ByteSequence producerIdBytes = marshaller.marshal(producerId);
         producerIdBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_PRODUCER_ID, producerIdBytes.data);
      }
      ByteSequence propBytes = messageSend.getMarshalledProperties();
      if (propBytes != null)
      {
         propBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_MARSHALL_PROP, propBytes.data);
         //unmarshall properties to core so selector will work
         Map<String, Object> props = messageSend.getProperties();
         //Map<String, Object> props = MarshallingSupport.unmarshalPrimitiveMap(new DataInputStream(new ByteArrayInputStream(propBytes)));
         Iterator<Entry<String, Object>> iterEntries = props.entrySet().iterator();
         while (iterEntries.hasNext())
         {
            Entry<String, Object> ent = iterEntries.next();

            Object value = ent.getValue();
            try
            {
               coreMessage.putObjectProperty(ent.getKey(), value);
            }
            catch (HornetQPropertyConversionException e)
            {
               coreMessage.putStringProperty(ent.getKey(), value.toString());
            }
         }
      }

      coreMessage.putIntProperty(AMQ_MSG_REDELIVER_COUNTER, messageSend.getRedeliveryCounter());
      ActiveMQDestination replyTo = messageSend.getReplyTo();
      if (replyTo != null)
      {
         ByteSequence replyToBytes = marshaller.marshal(replyTo);
         replyToBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_REPLY_TO, replyToBytes.data);
      }

      ConsumerId consumerId = messageSend.getTargetConsumerId();

      if (consumerId != null)
      {
         ByteSequence consumerIdBytes = marshaller.marshal(consumerId);
         consumerIdBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_CONSUMER_ID, consumerIdBytes.data);
      }
      TransactionId txId = messageSend.getTransactionId();
      if (txId != null)
      {
         ByteSequence txIdBytes = marshaller.marshal(txId);
         txIdBytes.compact();
         coreMessage.putBytesProperty(AMQ_MSG_TX_ID, txIdBytes.data);
      }

      String userId = messageSend.getUserID();
      if (userId != null)
      {
         coreMessage.putStringProperty(AMQ_MSG_USER_ID, userId);
      }
      coreMessage.putBooleanProperty(AMQ_MSG_COMPRESSED, messageSend.isCompressed());
      coreMessage.putBooleanProperty(AMQ_MSG_DROPPABLE, messageSend.isDroppable());
   }

   public static byte toCoreType(byte amqType)
   {
      switch (amqType)
      {
         case CommandTypes.ACTIVEMQ_BLOB_MESSAGE:
            throw new IllegalStateException("We don't support BLOB type yet!");
         case CommandTypes.ACTIVEMQ_BYTES_MESSAGE:
            return org.hornetq.api.core.Message.BYTES_TYPE;
         case CommandTypes.ACTIVEMQ_MAP_MESSAGE:
            return org.hornetq.api.core.Message.MAP_TYPE;
         case CommandTypes.ACTIVEMQ_OBJECT_MESSAGE:
            return org.hornetq.api.core.Message.OBJECT_TYPE;
         case CommandTypes.ACTIVEMQ_STREAM_MESSAGE:
            return org.hornetq.api.core.Message.STREAM_TYPE;
         case CommandTypes.ACTIVEMQ_TEXT_MESSAGE:
            return org.hornetq.api.core.Message.TEXT_TYPE;
         case CommandTypes.ACTIVEMQ_MESSAGE:
            return org.hornetq.api.core.Message.DEFAULT_TYPE;
         default:
            throw new IllegalStateException("Unknown ActiveMQ message type: " + amqType);
      }
   }

   public static SimpleString toCoreAddress(ActiveMQDestination dest)
   {
      if (dest.isQueue())
      {
         return new SimpleString("jms.queue." + dest.getPhysicalName());
      }
      else
      {
         return new SimpleString("jms.topic." + dest.getPhysicalName());
      }
   }

   public static MessageDispatch createMessageDispatch(ServerMessage message,
         int deliveryCount, AMQConsumer consumer) throws IOException
   {
      ActiveMQMessage amqMessage = toAMQMessage(message, consumer.getMarshaller());

      MessageDispatch md = new MessageDispatch();
      md.setConsumerId(consumer.getId());
      md.setMessage(amqMessage);
      md.setRedeliveryCounter(deliveryCount);
      ActiveMQDestination destination = amqMessage.getDestination();
      md.setDestination(destination);

      return md;
   }

   private static ActiveMQMessage toAMQMessage(ServerMessage coreMessage, WireFormat marshaller) throws IOException
   {
      ActiveMQMessage amqMsg = null;
      switch (coreMessage.getType())
      {
         case org.hornetq.api.core.Message.BYTES_TYPE:
            amqMsg = new ActiveMQBytesMessage();
            break;
         case org.hornetq.api.core.Message.MAP_TYPE:
            amqMsg = new ActiveMQMapMessage();
            break;
         case org.hornetq.api.core.Message.OBJECT_TYPE:
            amqMsg = new ActiveMQObjectMessage();
            break;
         case org.hornetq.api.core.Message.STREAM_TYPE:
            amqMsg = new ActiveMQStreamMessage();
            break;
         case org.hornetq.api.core.Message.TEXT_TYPE:
            amqMsg = new ActiveMQTextMessage();
            break;
         case org.hornetq.api.core.Message.DEFAULT_TYPE:
            amqMsg = new ActiveMQMessage();
            break;
         default:
            throw new IllegalStateException("Unknown message type: " + coreMessage.getType());
      }

      String type = coreMessage.getStringProperty(new SimpleString("JMSType"));
      if (type != null)
      {
         amqMsg.setJMSType(type);
      }
      amqMsg.setPersistent(coreMessage.isDurable());
      amqMsg.setExpiration(coreMessage.getExpiration());
      amqMsg.setPriority(coreMessage.getPriority());
      amqMsg.setTimestamp(coreMessage.getTimestamp());

      HornetQBuffer buffer = coreMessage.getBodyBuffer();
      buffer.resetReaderIndex();
      if (buffer != null)
      {
         int n = buffer.readableBytes();
         byte[] bytes = new byte[n];
         buffer.readBytes(bytes);
         buffer.resetReaderIndex();// this is important for topics as the buffer
                                   // may be read multiple times
         ByteSequence content = new ByteSequence(bytes);
         amqMsg.setContent(content);
      }

      amqMsg.setArrival(coreMessage.getLongProperty(AMQ_MSG_ARRIVAL));
      amqMsg.setBrokerInTime(coreMessage.getLongProperty(AMQ_MSG_BROKER_IN_TIME));

      String brokerPath = coreMessage.getStringProperty(AMQ_MSG_BROKER_PATH);
      if (brokerPath != null && brokerPath.isEmpty())
      {
         String[] brokers = brokerPath.split(",");
         BrokerId[] bids = new BrokerId[brokers.length];
         for (int i = 0; i < bids.length; i++)
         {
            bids[i] = new BrokerId(brokers[i]);
         }
         amqMsg.setBrokerPath(bids);
      }

      String clusterPath = coreMessage.getStringProperty(AMQ_MSG_CLUSTER);
      if (clusterPath != null && clusterPath.isEmpty())
      {
         String[] cluster = clusterPath.split(",");
         BrokerId[] bids = new BrokerId[cluster.length];
         for (int i = 0; i < bids.length; i++)
         {
            bids[i] = new BrokerId(cluster[i]);
         }
         amqMsg.setCluster(bids);
      }

      amqMsg.setCommandId(coreMessage.getIntProperty(AMQ_MSG_COMMAND_ID));
      String corrId = coreMessage.getStringProperty("JMSCorrelationID");
      if (corrId != null)
      {
         amqMsg.setCorrelationId(corrId);
      }

      byte[] dsBytes = coreMessage.getBytesProperty(AMQ_MSG_DATASTRUCTURE);
      if (dsBytes != null)
      {
         ByteSequence seq = new ByteSequence(dsBytes);
         DataStructure ds = (DataStructure)marshaller.unmarshal(seq);
         amqMsg.setDataStructure(ds);
      }

      byte[] destBytes = coreMessage.getBytesProperty(AMQ_MSG_DESTINATION);
      if (destBytes != null)
      {
         ByteSequence seq = new ByteSequence(destBytes);
         ActiveMQDestination dest = (ActiveMQDestination) marshaller.unmarshal(seq);
         amqMsg.setDestination(dest);
      }

      String groupId = coreMessage.getStringProperty(AMQ_MSG_GROUP_ID);
      if (groupId != null)
      {
         amqMsg.setGroupID(groupId);
      }
      amqMsg.setGroupSequence(coreMessage.getIntProperty(AMQ_MSG_GROUP_SEQUENCE));

      byte[] midBytes = coreMessage.getBytesProperty(AMQ_MSG_MESSAGE_ID);
      ByteSequence midSeq = new ByteSequence(midBytes);
      MessageId mid = (MessageId)marshaller.unmarshal(midSeq);
      amqMsg.setMessageId(mid);

      byte[] origDestBytes = coreMessage.getBytesProperty(AMQ_MSG_ORIG_DESTINATION);
      if (origDestBytes != null)
      {
         ActiveMQDestination origDest = (ActiveMQDestination) marshaller.unmarshal(new ByteSequence(origDestBytes));
         amqMsg.setOriginalDestination(origDest);
      }

      byte[] origTxIdBytes = coreMessage.getBytesProperty(AMQ_MSG_ORIG_TXID);
      if (origTxIdBytes != null)
      {
         TransactionId origTxId = (TransactionId) marshaller.unmarshal(new ByteSequence(origTxIdBytes));
         amqMsg.setOriginalTransactionId(origTxId);
      }

      byte[] producerIdBytes = coreMessage.getBytesProperty(AMQ_MSG_PRODUCER_ID);
      if (producerIdBytes != null)
      {
         ProducerId producerId = (ProducerId) marshaller.unmarshal(new ByteSequence(producerIdBytes));
         amqMsg.setProducerId(producerId);
      }

      byte[] marshalledBytes = coreMessage.getBytesProperty(AMQ_MSG_MARSHALL_PROP);
      if (marshalledBytes != null)
      {
         amqMsg.setMarshalledProperties(new ByteSequence(marshalledBytes));
      }

      amqMsg.setRedeliveryCounter(coreMessage.getIntProperty(AMQ_MSG_REDELIVER_COUNTER));

      byte[] replyToBytes = coreMessage.getBytesProperty(AMQ_MSG_REPLY_TO);
      if (replyToBytes != null)
      {
         ActiveMQDestination replyTo = (ActiveMQDestination) marshaller.unmarshal(new ByteSequence(replyToBytes));
         amqMsg.setReplyTo(replyTo);
      }

      byte[] consumerIdBytes = coreMessage.getBytesProperty(AMQ_MSG_CONSUMER_ID);
      if (consumerIdBytes != null)
      {
         ConsumerId consumerId = (ConsumerId) marshaller.unmarshal(new ByteSequence(consumerIdBytes));
         amqMsg.setTargetConsumerId(consumerId);
      }

      byte[] txIdBytes = coreMessage.getBytesProperty(AMQ_MSG_TX_ID);
      if (txIdBytes != null)
      {
         TransactionId txId = (TransactionId) marshaller.unmarshal(new ByteSequence(txIdBytes));
         amqMsg.setTransactionId(txId);
      }

      String userId = coreMessage.getStringProperty(AMQ_MSG_USER_ID);
      if (userId != null)
      {
         amqMsg.setUserID(userId);
      }

      amqMsg.setCompressed(coreMessage.getBooleanProperty(AMQ_MSG_COMPRESSED));
      amqMsg.setDroppable(coreMessage.getBooleanProperty(AMQ_MSG_DROPPABLE));

      return amqMsg;
   }

}
