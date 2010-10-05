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

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageInternal;
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
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(StompUtils.class);


   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

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
      Object replyTo = headers.remove(Stomp.Headers.Send.REPLY_TO);
      if (replyTo != null)
      {
         msg.putStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME, SimpleString.toSimpleString((String)replyTo));
      }
      String expiration = (String)headers.remove(Stomp.Headers.Send.EXPIRATION_TIME);
      if (expiration != null)
      {
         msg.setExpiration(Long.parseLong(expiration));
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

   public static void copyStandardHeadersFromMessageToFrame(MessageInternal message, StompFrame command, int deliveryCount) throws Exception
   {
      final Map<String, Object> headers = command.getHeaders();
      headers.put(Stomp.Headers.Message.DESTINATION, message.getAddress().toString());
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
                     message.getStringProperty(ClientMessageImpl.REPLYTO_HEADER_NAME));
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
         if (name.equals(ClientMessageImpl.REPLYTO_HEADER_NAME) ||
                  name.toString().equals("JMSType") ||
                  name.toString().equals("JMSCorrelationID"))
         {
            continue;
         }

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
