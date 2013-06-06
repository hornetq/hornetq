/*
 * Copyright 2013 Red Hat, Inc.
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

package org.hornetq.jms.client;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.hornetq.api.core.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * NOTE: this class forwards {@link #setDisableMessageID(boolean)} and
 * {@link #setDisableMessageTimestamp(boolean)} calls their equivalent at the
 * {@link MessageProducer}. IF the user is using the producer in async mode, this may lead to races.
 * We allow/tolerate this because these are just optional optimizations.
 * @author <a href="http://jmesnil.net/">Jeff Mesnil</a> (c) 2013 Red Hat inc.
 */
public class HornetQJMSProducer implements JMSProducer
{
   private final JMSContext context;
   private final MessageProducer producer;
   private final TypedProperties properties = new TypedProperties();

   private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
   private int priority = Message.DEFAULT_PRIORITY;
   private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
   private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;
   private volatile CompletionListener completionListener;

   private Destination jmsHeaderReplyTo;
   private String jmsHeaderCorrelationID;
   private byte[] jmsHeaderCorrelationIDAsBytes;
   private String jmsHeaderType;

   HornetQJMSProducer(JMSContext context, MessageProducer producer)
   {
      this.context = context;
      this.producer = producer;
   }

   @Override
   public JMSProducer send(Destination destination, Message message)
   {
      try
      {
         if (jmsHeaderCorrelationID != null)
         {
            message.setJMSCorrelationID(jmsHeaderCorrelationID);
         }
         if (jmsHeaderCorrelationIDAsBytes != null && jmsHeaderCorrelationIDAsBytes.length > 0)
         {
            message.setJMSCorrelationIDAsBytes(jmsHeaderCorrelationIDAsBytes);
         }
         if (jmsHeaderReplyTo != null)
         {
            message.setJMSReplyTo(jmsHeaderReplyTo);
         }
         if (jmsHeaderType != null)
         {
            message.setJMSType(jmsHeaderType);
         }
         // XXX HORNETQ-1209 "JMS 2.0" can this be a foreign msg?
         // if so, then "SimpleString" properties will trigger an error.
         setProperties(message);
         if (completionListener != null)
         {
            producer.send(destination, message, deliveryMode, priority, timeToLive, completionListener);
         }
         else
         {
            producer.send(destination, message, deliveryMode, priority, timeToLive);
         }
      }
      catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      return this;
   }

   /**
    * Sets all properties we carry onto the message.
    * @param message
    * @throws JMSException
    */
   private void setProperties(Message message) throws JMSException
   {
      for (SimpleString name : properties.getPropertyNames())
      {
         message.setObjectProperty(name.toString(), properties.getProperty(name));
      }
   }

   @Override
   public JMSProducer send(Destination destination, String body)
   {
      TextMessage message = context.createTextMessage(body);
      send(destination, message);
      return this;
   }

   @Override
   public JMSProducer send(Destination destination, Map<String, Object> body)
   {
      MapMessage message = context.createMapMessage();
      if (body != null)
      {
         try
         {
            for (Entry<String, Object> entry : body.entrySet())
            {
               final String name = entry.getKey();
               final Object v = entry.getValue();
               if (v instanceof String)
               {
                  message.setString(name, (String)v);
               }
               else if (v instanceof Long)
               {
                  message.setLong(name, (Long)v);
               }
               else if (v instanceof Double)
               {
                  message.setDouble(name, (Double)v);
               }
               else if (v instanceof Integer)
               {
                  message.setInt(name, (Integer)v);
               }
               else if (v instanceof Character)
               {
                  message.setChar(name, (Character)v);
               }
               else if (v instanceof Short)
               {
                  message.setShort(name, (Short)v);
               }
               else if (v instanceof Boolean)
               {
                  message.setBoolean(name, (Boolean)v);
               }
               else if (v instanceof Float)
               {
                  message.setFloat(name, (Float)v);
               }
               else if (v instanceof Byte)
               {
                  message.setByte(name, (Byte)v);
               }else if (v instanceof byte[])
               {
                byte[] array=  (byte[])v;
                  message.setBytes(name, array, 0, array.length);
               }
               else
               {
                  message.setObject(name, v);
               }
            }
         }
         catch (JMSException e)
         {
            throw new MessageFormatRuntimeException(e.getMessage());
         }
      }
      send(destination, message);
      return this;
   }

   @Override
   public JMSProducer send(Destination destination, byte[] body)
   {
      BytesMessage message = context.createBytesMessage();
      if (body != null)
      {
         try
         {
            message.writeBytes(body);
         }
         catch (JMSException e)
         {
            throw new MessageFormatRuntimeException(e.getMessage());
         }
      }
      send(destination, message);
      return this;
   }

   @Override
   public JMSProducer send(Destination destination, Serializable body)
   {
      ObjectMessage message = context.createObjectMessage(body);
      send(destination, message);
      return this;
   }

   @Override
   public JMSProducer setDisableMessageID(boolean value)
   {
      try
      {
         producer.setDisableMessageID(value);
      }
      catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      return this;
   }

   @Override
   public boolean getDisableMessageID()
   {
      try
      {
         return producer.getDisableMessageID();
      }
      catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSProducer setDisableMessageTimestamp(boolean value)
   {
      try
      {
         producer.setDisableMessageTimestamp(value);
      }
      catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      return this;
   }

   @Override
   public boolean getDisableMessageTimestamp()
   {
      try
      {
         return producer.getDisableMessageTimestamp();
      }
      catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSProducer setDeliveryMode(int deliveryMode)
   {
      if (deliveryMode != DeliveryMode.NON_PERSISTENT && deliveryMode != DeliveryMode.PERSISTENT)
      {
         throw new IllegalArgumentException("Illegal deliveryMode value: " + deliveryMode);
      }
      this.deliveryMode = deliveryMode;
      return this;
   }

   @Override
   public int getDeliveryMode()
   {
      return deliveryMode;
   }

   @Override
   public JMSProducer setPriority(int priority)
   {
      if (priority < 0 || priority > 9)
      {
         throw new IllegalArgumentException("Illegal priority value: " + priority);
      }
      this.priority = priority;
      return this;
   }

   @Override
   public int getPriority()
   {
      return priority;
   }

   @Override
   public JMSProducer setTimeToLive(long timeToLive)
   {
      this.timeToLive = timeToLive;
      return this;
   }

   @Override
   public long getTimeToLive()
   {
      return timeToLive;
   }

   @Override
   public JMSProducer setDeliveryDelay(long deliveryDelay)
   {
      this.deliveryDelay = deliveryDelay;
      return this;
   }

   @Override
   public long getDeliveryDelay()
   {
      return deliveryDelay;
   }

   @Override
   public JMSProducer setAsync(CompletionListener completionListener)
   {
      this.completionListener = completionListener;
      return this;
   }

   @Override
   public CompletionListener getAsync()
   {
      return completionListener;
   }

   @Override
   public JMSProducer setProperty(String name, boolean value)
   {
      properties.putBooleanProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, byte value)
   {
      properties.putByteProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, short value)
   {
      properties.putShortProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, int value)
   {
      properties.putIntProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, long value)
   {
      properties.putLongProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, float value)
   {
      properties.putFloatProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, double value)
   {
      properties.putDoubleProperty(new SimpleString(name), value);
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, String value)
   {
      properties.putSimpleStringProperty(new SimpleString(name), new SimpleString(value));
      return this;
   }

   @Override
   public JMSProducer setProperty(String name, Object value)
   {
      try
      {
         TypedProperties.setObjectProperty(new SimpleString(name), value, properties);
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
      return this;
   }

   @Override
   public JMSProducer clearProperties()
   {
      try
      {
         properties.clear();
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
      return this;
   }

   @Override
   public boolean propertyExists(String name)
   {
      return properties.containsProperty(new SimpleString(name));
   }

   @Override
   public boolean getBooleanProperty(String name)
   {
      try
      {
         return properties.getBooleanProperty(new SimpleString(name));
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public byte getByteProperty(String name)
   {
      try
      {
         return properties.getByteProperty(new SimpleString(name));
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public short getShortProperty(String name)
   {
      try
      {
         return properties.getShortProperty(new SimpleString(name));
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public int getIntProperty(String name)
   {
      try
      {
         return properties.getIntProperty(new SimpleString(name));
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public long getLongProperty(String name)
   {
      try
      {
         return properties.getLongProperty(new SimpleString(name));
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public float getFloatProperty(String name)
   {
      try
      {
         return properties.getFloatProperty(new SimpleString(name));
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public double getDoubleProperty(String name)
   {
      try
      {
         return properties.getDoubleProperty(new SimpleString(name));
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public String getStringProperty(String name)
   {
      try
      {
         SimpleString prop = properties.getSimpleStringProperty(new SimpleString(name));
         if (prop == null)
            return null;
         return prop.toString();
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public Object getObjectProperty(String name)
   {
      try
      {
         return properties.getProperty(new SimpleString(name));
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public Set<String> getPropertyNames()
   {
      try
      {
         Set<String> propNames = new HashSet<String>();

         for (SimpleString str : properties.getPropertyNames())
         {
            propNames.add(str.toString());
         }
         return propNames;
      }
      catch (RuntimeException e)
      {
         throw new JMSRuntimeException(e.getMessage());
      }
   }

   @Override
   public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID)
   {
      if (correlationID == null || correlationID.length == 0)
      {
         throw new JMSRuntimeException("Please specify a non-zero length byte[]");
      }
      jmsHeaderCorrelationIDAsBytes = Arrays.copyOf(correlationID, correlationID.length);
      return this;
   }

   @Override
   public byte[] getJMSCorrelationIDAsBytes()
   {
      return Arrays.copyOf(jmsHeaderCorrelationIDAsBytes, jmsHeaderCorrelationIDAsBytes.length);
   }

   @Override
   public JMSProducer setJMSCorrelationID(String correlationID)
   {
      jmsHeaderCorrelationID = correlationID;
      return this;
   }

   @Override
   public String getJMSCorrelationID()
   {
      return jmsHeaderCorrelationID;
   }

   @Override
   public JMSProducer setJMSType(String type)
   {
      jmsHeaderType = type;
      return this;
   }

   @Override
   public String getJMSType()
   {
      return jmsHeaderType;
   }

   @Override
   public JMSProducer setJMSReplyTo(Destination replyTo)
   {
      jmsHeaderReplyTo = replyTo;
      return this;
   }

   @Override
   public Destination getJMSReplyTo()
   {
      return jmsHeaderReplyTo;
   }
}
