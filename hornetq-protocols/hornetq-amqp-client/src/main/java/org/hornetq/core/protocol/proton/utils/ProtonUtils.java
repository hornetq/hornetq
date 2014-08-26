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

package org.hornetq.core.protocol.proton.utils;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.CompositeWritableBuffer;
import org.apache.qpid.proton.codec.DroppingWritableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageFormat;
import org.apache.qpid.proton.message.impl.MessageImpl;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.proton.client.MessageCreator;
import org.hornetq.utils.TypedProperties;

import static org.hornetq.api.core.Message.TEXT_TYPE;

/**
 * M could be either ClientMessage or ServerMessage depending on where it's being used.
 * C is either Client or Server Connector
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         4/11/13
 */
public class ProtonUtils<M extends MessageInternal, C extends MessageCreator<M>>
{
   private static final String PREFIX = "HORNETQ_PROTON_";
   private static final String MESSAGE_ANNOTATIONS = PREFIX + "MESSAGE_ANNOTATIONS_";
   private static final String DELIVERY_ANNOTATIONS = PREFIX + "DELIVERY_ANNOTATIONS_";
   private static final String FOOTER_VALUES = PREFIX + "FOOTER_VALUES";
   private static final String MESSAGE_FORMAT = PREFIX + "MESSAGE_FORMAT";
   private static final String PROTON_MESSAGE_FORMAT = PREFIX + "FORMAT";
   private static final String PROTON_MESSAGE_SIZE = "PROTON_MESSAGE_SIZE";
   private static final String MESSAGE_TYPE = PREFIX + "MESSAGE_TYPE";
   private static final String FIRST_ACQUIRER = PREFIX + "FIRST_ACQUIRER";
   private static final String USER_ID = PREFIX + "USER_ID";
   private static final String SUBJECT = PREFIX + "SUBJECT";
   private static final String REPLY_TO = PREFIX + "REPLY_TO";
   private static final String CORRELATION_ID = PREFIX + "CORRELATION_ID";
   private static final String CONTENT_TYPE = PREFIX + "CONTENT_TYPE";
   private static final String CONTENT_ENCODING = PREFIX + "CONTENT_TYPE";
   private static final String ABSOLUTE_EXPIRY_TIME = PREFIX + "ABSOLUTE_EXPIRY_TIME";
   private static final String CREATION_TIME = PREFIX + "CREATION_TIME";
   private static final String GROUP_ID = PREFIX + "GROUP_ID";
   private static final String GROUP_SEQUENCE = PREFIX + "GROUP_SEQUENCE";
   private static final String REPLY_TO_GROUP_ID = PREFIX + "REPLY_TO_GROUP_ID";

   private static final SimpleString USER_ID_SS = new SimpleString(USER_ID);
   private static final SimpleString SUBJECT_SS = new SimpleString(SUBJECT);
   private static final SimpleString REPLY_TO_SS = new SimpleString(REPLY_TO);
   private static final SimpleString CORRELATION_ID_SS = new SimpleString(CORRELATION_ID);
   private static final SimpleString CONTENT_TYPE_SS = new SimpleString(CONTENT_TYPE);
   private static final SimpleString CONTENT_ENCODING_SS = new SimpleString(CONTENT_ENCODING);
   private static final SimpleString ABSOLUTE_EXPIRY_TIME_SS = new SimpleString(ABSOLUTE_EXPIRY_TIME);
   private static final SimpleString CREATION_TIME_SS = new SimpleString(CREATION_TIME);
   private static final SimpleString GROUP_ID_SS = new SimpleString(GROUP_ID);
   private static final SimpleString GROUP_SEQUENCE_SS = new SimpleString(GROUP_SEQUENCE);
   private static final SimpleString REPLY_TO_GROUP_ID_SS = new SimpleString(REPLY_TO_GROUP_ID);
   private static final SimpleString PROTON_MESSAGE_SIZE_SS = new SimpleString(PROTON_MESSAGE_SIZE);

   private static Set<String> SPECIAL_PROPS = new HashSet<String>();

   static
   {
      SPECIAL_PROPS.add(MESSAGE_FORMAT);
      SPECIAL_PROPS.add(MESSAGE_TYPE);
      SPECIAL_PROPS.add(FIRST_ACQUIRER);
      SPECIAL_PROPS.add(PROTON_MESSAGE_FORMAT);
      SPECIAL_PROPS.add(PROTON_MESSAGE_SIZE);
      SPECIAL_PROPS.add(MESSAGE_TYPE);
      SPECIAL_PROPS.add(USER_ID);
      SPECIAL_PROPS.add(SUBJECT);
      SPECIAL_PROPS.add(REPLY_TO);
      SPECIAL_PROPS.add(CORRELATION_ID);
      SPECIAL_PROPS.add(CONTENT_TYPE);
      SPECIAL_PROPS.add(CONTENT_ENCODING);
      SPECIAL_PROPS.add(ABSOLUTE_EXPIRY_TIME);
      SPECIAL_PROPS.add(CREATION_TIME);
      SPECIAL_PROPS.add(GROUP_ID);
      SPECIAL_PROPS.add(GROUP_SEQUENCE);
      SPECIAL_PROPS.add(REPLY_TO_GROUP_ID);
   }

   private INBOUND inbound = new INBOUND();
   public INBOUND getInbound()
   {
      return inbound;
   }

   public class INBOUND
   {
      public M transform(C connection, EncodedMessage encodedMessage) throws Exception
      {
         Message protonMessage = encodedMessage.decode();

         Header header = protonMessage.getHeader();
         if (header == null)
         {
            header = new Header();
         }

         M message = connection.createMessage();
         TypedProperties properties = message.getTypedProperties();

         properties.putLongProperty(new SimpleString(MESSAGE_FORMAT), encodedMessage.getMessageFormat());
         properties.putLongProperty(new SimpleString(PROTON_MESSAGE_FORMAT), getMessageFormat(protonMessage.getMessageFormat()));
         properties.putIntProperty(new SimpleString(PROTON_MESSAGE_SIZE), encodedMessage.getLength());

         populateSpecialProps(header, protonMessage, message, properties);
         populateHeaderProperties(header, properties, message);
         populateDeliveryAnnotations(protonMessage.getDeliveryAnnotations(), properties);
         populateMessageAnnotations(protonMessage.getMessageAnnotations(), properties);
         populateApplicationProperties(protonMessage.getApplicationProperties(), properties);
         populateProperties(protonMessage.getProperties(), properties, message);
         populateFooterProperties(protonMessage.getFooter(), properties);
         message.setTimestamp(System.currentTimeMillis());

         Section section = protonMessage.getBody();
         if (section instanceof AmqpValue)
         {
            AmqpValue amqpValue = (AmqpValue) section;
            Object value = amqpValue.getValue();
            if (value instanceof String)
            {
               message.getBodyBuffer().writeNullableString((String) value);
            }
            else if (value instanceof Binary)
            {
               Binary binary = (Binary) value;
               message.getBodyBuffer().writeBytes(binary.getArray());
            }
         }
         else if (section instanceof Data)
         {
            message.getBodyBuffer().writeBytes(((Data) section).getValue().getArray());
         }

         return message;
      }

      private void populateSpecialProps(Header header, Message protonMessage, M message, TypedProperties properties)
      {
         if (header.getFirstAcquirer() != null)
         {
            properties.putBooleanProperty(new SimpleString(FIRST_ACQUIRER), header.getFirstAcquirer());
         }
         properties.putIntProperty(new SimpleString(MESSAGE_TYPE), getMessageType(protonMessage));
      }

      private void populateHeaderProperties(Header header, TypedProperties properties, M message)
      {
         if (header.getDurable() != null)
         {
            message.setDurable(header.getDurable());
         }

         if (header.getPriority() != null)
         {
            message.setPriority((byte) header.getPriority().intValue());
         }

         if (header.getTtl() != null)
         {
            message.setExpiration(header.getTtl().longValue());
         }
      }

      private void populateDeliveryAnnotations(DeliveryAnnotations deliveryAnnotations, TypedProperties properties)
      {
         if (deliveryAnnotations != null)
         {
            Map values = deliveryAnnotations.getValue();
            Set keySet = values.keySet();
            for (Object key : keySet)
            {
               Symbol symbol = (Symbol) key;
               Object value = values.get(key);
               properties.putSimpleStringProperty(new SimpleString(DELIVERY_ANNOTATIONS + symbol.toString()), new SimpleString(value.toString()));
            }
         }
      }

      private void populateFooterProperties(Footer footer, TypedProperties properties)
      {
         if (footer != null)
         {
            Map values = footer.getValue();
            Set keySet = values.keySet();
            for (Object key : keySet)
            {
               Symbol symbol = (Symbol) key;
               Object value = values.get(key);
               properties.putSimpleStringProperty(new SimpleString(FOOTER_VALUES + symbol.toString()), new SimpleString(value.toString()));
            }
         }
      }

      private void populateProperties(Properties amqpProperties, TypedProperties properties, M message)
      {
         if (amqpProperties == null)
         {
            return;
         }
         if (amqpProperties.getTo() != null)
         {
            message.setAddress(new SimpleString(amqpProperties.getTo()));
         }
         if (amqpProperties.getUserId() != null)
         {
            properties.putBytesProperty(USER_ID_SS, amqpProperties.getUserId().getArray());
         }
         if (amqpProperties.getSubject() != null)
         {
            properties.putSimpleStringProperty(SUBJECT_SS, new SimpleString(amqpProperties.getSubject()));
         }
         if (amqpProperties.getReplyTo() != null)
         {
            properties.putSimpleStringProperty(REPLY_TO_SS, new SimpleString(amqpProperties.getReplyTo()));
         }
         if (amqpProperties.getCorrelationId() != null)
         {
            properties.putSimpleStringProperty(CORRELATION_ID_SS, new SimpleString(amqpProperties.getCorrelationId().toString()));
         }
         if (amqpProperties.getContentType() != null)
         {
            properties.putSimpleStringProperty(CONTENT_TYPE_SS, new SimpleString(amqpProperties.getContentType().toString()));
         }
         if (amqpProperties.getContentEncoding() != null)
         {
            properties.putSimpleStringProperty(CONTENT_ENCODING_SS, new SimpleString(amqpProperties.getContentEncoding().toString()));
         }
         if (amqpProperties.getAbsoluteExpiryTime() != null)
         {
            properties.putLongProperty(ABSOLUTE_EXPIRY_TIME_SS, amqpProperties.getAbsoluteExpiryTime().getTime());
         }
         if (amqpProperties.getCreationTime() != null)
         {
            properties.putLongProperty(CREATION_TIME_SS, amqpProperties.getCreationTime().getTime());
         }
         if (amqpProperties.getGroupId() != null)
         {
            properties.putSimpleStringProperty(GROUP_ID_SS, new SimpleString(amqpProperties.getGroupId()));
         }
         if (amqpProperties.getGroupSequence() != null)
         {
            properties.putIntProperty(GROUP_SEQUENCE_SS, amqpProperties.getGroupSequence().intValue());
         }
         if (amqpProperties.getReplyToGroupId() != null)
         {
            message.getTypedProperties().putSimpleStringProperty(REPLY_TO_GROUP_ID_SS, new SimpleString(amqpProperties.getReplyToGroupId()));
         }
      }

      private void populateApplicationProperties(ApplicationProperties applicationProperties, TypedProperties properties)
      {
         if (applicationProperties != null)
         {
            Map props = applicationProperties.getValue();
            for (Object key : props.keySet())
            {
               Object val = props.get(key);
               setProperty(key, val, properties);
            }
         }
      }

      private void setProperty(Object key, Object val, TypedProperties properties)
      {
         if (val instanceof String)
         {
            properties.putSimpleStringProperty(new SimpleString((String) key), new SimpleString((String) val));
         }
         else if (val instanceof Boolean)
         {
            properties.putBooleanProperty(new SimpleString((String) key), (Boolean) val);
         }
         else if (val instanceof Double)
         {
            properties.putDoubleProperty(new SimpleString((String) key), (Double) val);
         }
         else if (val instanceof Float)
         {
            properties.putFloatProperty(new SimpleString((String) key), (Float) val);
         }
         else if (val instanceof Integer)
         {
            properties.putIntProperty(new SimpleString((String) key), (Integer) val);
         }
         else if (val instanceof Byte)
         {
            properties.putByteProperty(new SimpleString((String) key), (Byte) val);
         }
      }

      public void populateMessageAnnotations(MessageAnnotations messageAnnotations, TypedProperties properties)
      {
         if (messageAnnotations != null)
         {
            Map values = messageAnnotations.getValue();
            Set keySet = values.keySet();
            for (Object key : keySet)
            {
               Symbol symbol = (Symbol) key;
               Object value = values.get(key);
               properties.putSimpleStringProperty(new SimpleString(MESSAGE_ANNOTATIONS + symbol.toString()), new SimpleString(value.toString()));
            }
         }
      }

   }

   private OUTBOUND outbound = new OUTBOUND();

   public OUTBOUND getOutbound()
   {
      return outbound;
   }

   public class OUTBOUND
   {
      public EncodedMessage transform(M message, int deliveryCount)
      {
         long messageFormat = 0;

         if (message.containsProperty(MESSAGE_FORMAT))
         {
            messageFormat = message.getLongProperty(MESSAGE_FORMAT);
         }
         System.out.println("Message format: " + messageFormat);

         Integer size;
         if (message.containsProperty(PROTON_MESSAGE_SIZE_SS))
         {
            size = message.getIntProperty(PROTON_MESSAGE_SIZE_SS);
         }
         else
         {
            size = message.getEncodeSize(); // just an estimate for now
         }

         Header header = populateHeader(message, deliveryCount);
         DeliveryAnnotations deliveryAnnotations = populateDeliveryAnnotations(message);
         MessageAnnotations messageAnnotations = populateMessageAnnotations(message);
         Properties props = populateProperties(message);
         ApplicationProperties applicationProperties = populateApplicationProperties(message);
         Section section = populateBody(message);
         Footer footer = populateFooter(message);
         Set<SimpleString> propertyNames = message.getPropertyNames();
         for (SimpleString propertyName : propertyNames)
         {
            TypedProperties typedProperties = message.getTypedProperties();
            String realName = propertyName.toString();
            if (realName.startsWith(MESSAGE_ANNOTATIONS))
            {

               SimpleString value = (SimpleString) typedProperties.getProperty(propertyName);
               Symbol symbol = Symbol.getSymbol(realName.replace(MESSAGE_ANNOTATIONS, ""));
               messageAnnotations.getValue().put(symbol, value.toString());
            }
         }
         MessageImpl protonMessage = new MessageImpl(header, deliveryAnnotations, messageAnnotations, props, applicationProperties, section, footer);
         protonMessage.setMessageFormat(getMessageFormat(messageFormat));
         ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
         final DroppingWritableBuffer overflow = new DroppingWritableBuffer();
         int c = protonMessage.encode(new CompositeWritableBuffer(new WritableBuffer.ByteBufferWrapper(buffer), overflow));
         if (overflow.position() > 0)
         {
            buffer = ByteBuffer.wrap(new byte[1024 * 4 + overflow.position()]);
            c = protonMessage.encode(new WritableBuffer.ByteBufferWrapper(buffer));
         }

         return new EncodedMessage(messageFormat, buffer.array(), 0, c);
      }

      private Header populateHeader(M message, int deliveryCount)
      {
         Header header = new Header();
         header.setDurable(message.isDurable());
         header.setPriority(new UnsignedByte(message.getPriority()));
         header.setDeliveryCount(new UnsignedInteger(deliveryCount));
         header.setTtl(new UnsignedInteger((int) message.getExpiration()));
         return header;
      }

      private DeliveryAnnotations populateDeliveryAnnotations(M message)
      {
         HashMap actualValues = new HashMap();
         DeliveryAnnotations deliveryAnnotations = new DeliveryAnnotations(actualValues);
         for (SimpleString name : message.getPropertyNames())
         {
            String sName = name.toString();
            if (sName.startsWith(DELIVERY_ANNOTATIONS))
            {
               Object val = message.getTypedProperties().getProperty(name);
               if (val instanceof SimpleString)
               {
                  actualValues.put(sName.subSequence(sName.indexOf(DELIVERY_ANNOTATIONS), sName.length()), val.toString());
               }
               else
               {
                  actualValues.put(sName.subSequence(sName.indexOf(DELIVERY_ANNOTATIONS), sName.length()), val);
               }
            }
         }
         //this is a proton jms thing, if not null it creates wrong type of message
         return actualValues.size() > 0 ? deliveryAnnotations : null;
      }

      private MessageAnnotations populateMessageAnnotations(M message)
      {
         HashMap actualValues = new HashMap();
         MessageAnnotations messageAnnotations = new MessageAnnotations(actualValues);
         for (SimpleString name : message.getPropertyNames())
         {
            String sName = name.toString();
            if (sName.startsWith(MESSAGE_ANNOTATIONS))
            {
               Object val = message.getTypedProperties().getProperty(name);
               if (val instanceof SimpleString)
               {
                  actualValues.put(sName.subSequence(sName.indexOf(MESSAGE_ANNOTATIONS), sName.length()), val.toString());
               }
               else
               {
                  actualValues.put(sName.subSequence(sName.indexOf(MESSAGE_ANNOTATIONS), sName.length()), val);
               }
            }
         }
         return messageAnnotations;
      }

      private Properties populateProperties(M message)
      {
         Calendar calendar = Calendar.getInstance();
         Properties properties = new Properties();
         TypedProperties typedProperties = message.getTypedProperties();
         properties.setMessageId(message.getMessageID());
         if (message.getAddress() != null)
         {
            properties.setTo(message.getAddress().toString());
         }
         if (typedProperties.containsProperty(USER_ID_SS))
         {
            properties.setUserId(new Binary(typedProperties.getBytesProperty(USER_ID_SS)));
         }
         if (typedProperties.containsProperty(SUBJECT_SS))
         {
            properties.setSubject(typedProperties.getSimpleStringProperty(SUBJECT_SS).toString());
         }
         if (typedProperties.containsProperty(REPLY_TO_SS))
         {
            properties.setReplyTo(typedProperties.getSimpleStringProperty(REPLY_TO_SS).toString());
         }
         if (typedProperties.containsProperty(CORRELATION_ID_SS))
         {
            properties.setCorrelationId(typedProperties.getSimpleStringProperty(CORRELATION_ID_SS).toString());
         }
         if (typedProperties.containsProperty(CONTENT_TYPE_SS))
         {
            properties.setContentType(Symbol.getSymbol(typedProperties.getSimpleStringProperty(CONTENT_TYPE_SS).toString()));
         }
         if (typedProperties.containsProperty(CONTENT_ENCODING_SS))
         {
            properties.setContentEncoding(Symbol.getSymbol(typedProperties.getSimpleStringProperty(CONTENT_ENCODING_SS).toString()));
         }
         if (typedProperties.containsProperty(ABSOLUTE_EXPIRY_TIME_SS))
         {
            calendar.setTimeInMillis(typedProperties.getLongProperty(ABSOLUTE_EXPIRY_TIME_SS));
            properties.setAbsoluteExpiryTime(calendar.getTime());
         }
         if (typedProperties.containsProperty(CREATION_TIME_SS))
         {
            calendar.setTimeInMillis(typedProperties.getLongProperty(CREATION_TIME_SS));
            properties.setCreationTime(calendar.getTime());
         }
         if (typedProperties.containsProperty(GROUP_ID_SS))
         {
            properties.setGroupId(typedProperties.getSimpleStringProperty(GROUP_ID_SS).toString());
         }
         if (typedProperties.containsProperty(GROUP_SEQUENCE_SS))
         {
            properties.setGroupSequence(new UnsignedInteger(typedProperties.getIntProperty(GROUP_SEQUENCE_SS)));
         }
         if (typedProperties.containsProperty(REPLY_TO_GROUP_ID_SS))
         {
            properties.setReplyToGroupId(typedProperties.getSimpleStringProperty(REPLY_TO_GROUP_ID_SS).toString());
         }
         return properties;
      }

      private ApplicationProperties populateApplicationProperties(M message)
      {
         HashMap<String, Object> values = new HashMap<String, Object>();
         for (SimpleString name : message.getPropertyNames())
         {
            setProperty(name, message.getTypedProperties().getProperty(name), values);
         }
         return new ApplicationProperties(values);
      }

      private void setProperty(SimpleString name, Object property, HashMap<String, Object> values)
      {
         String s = name.toString();
         if (SPECIAL_PROPS.contains(s) ||
            s.startsWith(MESSAGE_ANNOTATIONS) ||
            s.startsWith(DELIVERY_ANNOTATIONS) ||
            s.startsWith(FOOTER_VALUES))
         {
            return;
         }
         if (property instanceof SimpleString)
         {
            values.put(s, property.toString());
         }
         else
         {
            values.put(s, property);
         }
      }

      private Footer populateFooter(M message)
      {
         HashMap actualValues = new HashMap();
         Footer footer = new Footer(actualValues);
         for (SimpleString name : message.getPropertyNames())
         {
            String sName = name.toString();
            if (sName.startsWith(FOOTER_VALUES))
            {
               Object val = message.getTypedProperties().getProperty(name);
               if (val instanceof SimpleString)
               {
                  actualValues.put(sName.subSequence(sName.indexOf(FOOTER_VALUES), sName.length()), val.toString());
               }
               else
               {
                  actualValues.put(sName.subSequence(sName.indexOf(FOOTER_VALUES), sName.length()), val);
               }
            }
         }
         return footer;
      }

      private Section populateBody(M message)
      {
         // TODO: Depend on array() is most likely not a very good idea
         Integer type;

         try
         {
            type = message.getIntProperty(MESSAGE_TYPE);
         }
         catch (NumberFormatException e)
         {
            type = 0;
         }
         switch (type)
         {
            case 0:
            case 1:
               return new Data(new Binary(message.getBodyBuffer().copy().byteBuf().array()));
            case 2:
               return new AmqpValue(new Binary(message.getBodyBuffer().copy().byteBuf().array()));
            case 3:
               return new AmqpValue(message.getBodyBuffer().copy().readNullableString());
            default:
               return new Data(new Binary(message.getBodyBuffer().copy().byteBuf().array()));
         }
      }
   }

   private static long getMessageFormat(MessageFormat messageFormat)
   {
      switch (messageFormat)
      {
         case AMQP:
            return 0;
         case DATA:
            return 1;
         case JSON:
            return 2;
         case TEXT:
            return 3;
         default:
            return 0;

      }
   }

   private static MessageFormat getMessageFormat(long messageFormat)
   {
      switch ((int) messageFormat)
      {
         case 0:
            return MessageFormat.AMQP;
         case 1:
            return MessageFormat.DATA;
         case 2:
            return MessageFormat.JSON;
         case 3:
            return MessageFormat.TEXT;
         default:
            return MessageFormat.AMQP;

      }
   }

   private static int getMessageType(Message protonMessage)
   {
      Section section = protonMessage.getBody();
      if (section instanceof AmqpValue)
      {
         AmqpValue amqpValue = (AmqpValue) section;
         Object value = amqpValue.getValue();
         if (value instanceof String)
         {
            return TEXT_TYPE;
         }
         else if (value instanceof byte[])
         {
            return org.hornetq.api.core.Message.BYTES_TYPE;
         }
         else if (value instanceof Map)
         {
            return org.hornetq.api.core.Message.MAP_TYPE;
         }
         else if (value instanceof Object)
         {
            return org.hornetq.api.core.Message.OBJECT_TYPE;
         }
         else
         {
            return org.hornetq.api.core.Message.DEFAULT_TYPE;
         }
      }
      else
      {
         return org.hornetq.api.core.Message.DEFAULT_TYPE;
      }
   }
}
