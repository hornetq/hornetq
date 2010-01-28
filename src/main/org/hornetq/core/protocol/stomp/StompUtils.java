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
import java.util.Set;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;

/**
 * A StompUtils
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
class StompUtils
{

   public static String HQ_QUEUE_PREFIX = "jms.queue.";

   public static String STOMP_QUEUE_PREFIX = "/queue/";

   public static String HQ_TEMP_QUEUE_PREFIX = "jms.tempqueue.";

   public static String STOMP_TEMP_QUEUE_PREFIX = "/temp-queue/";

   public static String HQ_TOPIC_PREFIX = "jms.topic.";

   public static String STOMP_TOPIC_PREFIX = "/topic/";

   public static String HQ_TEMP_TOPIC_PREFIX = "jms.temptopic.";

   public static String STOMP_TEMP_TOPIC_PREFIX = "/temp-topic/";

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String toHornetQAddress(String stompDestination) throws HornetQException
   {
      if (stompDestination == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "No destination is specified!");
      }
      else if (stompDestination.startsWith(STOMP_QUEUE_PREFIX))
      {
         return convert(stompDestination, STOMP_QUEUE_PREFIX, HQ_QUEUE_PREFIX);
      }
      else if (stompDestination.startsWith(STOMP_TOPIC_PREFIX))
      {
         return convert(stompDestination, STOMP_TOPIC_PREFIX, HQ_TOPIC_PREFIX);
      }
      else if (stompDestination.startsWith(STOMP_TEMP_QUEUE_PREFIX))
      {
         return convert(stompDestination, STOMP_TEMP_QUEUE_PREFIX, HQ_TEMP_QUEUE_PREFIX);
      }
      else if (stompDestination.startsWith(STOMP_TEMP_TOPIC_PREFIX))
      {
         return convert(stompDestination, STOMP_TEMP_TOPIC_PREFIX, HQ_TEMP_TOPIC_PREFIX);
      }
      else
      {
         // it is also possible the STOMP client send a message directly to a HornetQ address
         // in that case, we do nothing:
         return stompDestination;
      }
   }

   public static String toStompDestination(String hornetqAddress) throws HornetQException
   {
      if (hornetqAddress == null)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "No destination is specified!");
      }
      else if (hornetqAddress.startsWith(HQ_QUEUE_PREFIX))
      {
         return convert(hornetqAddress, HQ_QUEUE_PREFIX, STOMP_QUEUE_PREFIX);
      }
      else if (hornetqAddress.startsWith(HQ_TOPIC_PREFIX))
      {
         return convert(hornetqAddress, HQ_TOPIC_PREFIX, STOMP_TOPIC_PREFIX);
      }
      else if (hornetqAddress.startsWith(HQ_TEMP_QUEUE_PREFIX))
      {
         return convert(hornetqAddress, HQ_TEMP_QUEUE_PREFIX, STOMP_TEMP_QUEUE_PREFIX);
      }
      else if (hornetqAddress.startsWith(HQ_TEMP_TOPIC_PREFIX))
      {
         return convert(hornetqAddress, HQ_TEMP_TOPIC_PREFIX, STOMP_TEMP_TOPIC_PREFIX);
      }
      else
      {
         // do nothing
         return hornetqAddress;
      }
   }

   private static String convert(String str, String oldPrefix, String newPrefix)
   {
      String sub = str.substring(oldPrefix.length(), str.length());
      return new String(newPrefix + sub);
   }

   public static void copyStandardHeadersFromFrameToMessage(StompFrame frame, ServerMessageImpl msg) throws Exception
   {
      Map<String, Object> headers = new HashMap<String, Object>(frame.getHeaders());

      String priority = (String)headers.remove(Stomp.Headers.Send.PRIORITY);
      if (priority != null)
      {
         msg.setPriority(Byte.parseByte(priority));
      }
      String persistent = (String)headers.remove(Stomp.Headers.Send.PERSISTENT);
      if (persistent != null)
      {
         msg.setDurable(Boolean.parseBoolean(persistent));
      }
      // FIXME should use a proper constant
      msg.putObjectProperty("JMSCorrelationID", headers.remove(Stomp.Headers.Send.CORRELATION_ID));
      msg.putObjectProperty("JMSType", headers.remove(Stomp.Headers.Send.TYPE));

      String groupID = (String)headers.remove("JMSXGroupID");
      if (groupID != null)
      {
         msg.putStringProperty(Message.HDR_GROUP_ID, SimpleString.toSimpleString(groupID));
      }
      Object o = headers.remove(Stomp.Headers.Send.REPLY_TO);
      if (o != null)
      {
         msg.putStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME, SimpleString.toSimpleString((String)o));
      }

      // now the general headers
      for (Iterator<Map.Entry<String, Object>> iter = headers.entrySet().iterator(); iter.hasNext();)
      {
         Map.Entry<String, Object> entry = iter.next();
         String name = (String)entry.getKey();
         Object value = entry.getValue();
         msg.putObjectProperty(name, value);
      }
   }

   public static void copyStandardHeadersFromMessageToFrame(Message message, StompFrame command, int deliveryCount) throws Exception
   {
      final Map<String, Object> headers = command.getHeaders();
      headers.put(Stomp.Headers.Message.DESTINATION, toStompDestination(message.getAddress().toString()));
      headers.put(Stomp.Headers.Message.MESSAGE_ID, message.getMessageID());

      if (message.getObjectProperty("JMSCorrelationID") != null)
      {
         headers.put(Stomp.Headers.Message.CORRELATION_ID, message.getObjectProperty("JMSCorrelationID"));
      }
      headers.put(Stomp.Headers.Message.EXPIRATION_TIME, "" + message.getExpiration());
      headers.put(Stomp.Headers.Message.REDELIVERED, deliveryCount > 1);
      headers.put(Stomp.Headers.Message.PRORITY, "" + message.getPriority());

      if (message.getStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME) != null)
      {
         headers.put(Stomp.Headers.Message.REPLY_TO,
                     toStompDestination(message.getStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME)));
      }
      headers.put(Stomp.Headers.Message.TIMESTAMP, "" + message.getTimestamp());

      if (message.getObjectProperty("JMSType") != null)
      {
         headers.put(Stomp.Headers.Message.TYPE, message.getObjectProperty("JMSType"));
      }

      // now lets add all the message headers
      Set<SimpleString> names = message.getPropertyNames();
      for (SimpleString name : names)
      {
         headers.put(name.toString(), message.getObjectProperty(name));
      }
   }
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
