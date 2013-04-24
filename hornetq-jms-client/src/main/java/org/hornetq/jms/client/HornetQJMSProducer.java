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
import java.util.Map;
import java.util.Set;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;

/**
 * @author <a href="http://jmesnil.net/">Jeff Mesnil</a> (c) 2013 Red Hat inc.
 */
public class HornetQJMSProducer implements JMSProducer
{

   private final JMSContext context;
   private final MessageProducer producer;

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
         producer.send(destination, message);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      return this;
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
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer send(Destination destination, byte[] body)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer send(Destination destination, Serializable body)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setDisableMessageID(boolean value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public boolean getDisableMessageID()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setDisableMessageTimestamp(boolean value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public boolean getDisableMessageTimestamp()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setDeliveryMode(int deliveryMode)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public int getDeliveryMode()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setPriority(int priority)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public int getPriority()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setTimeToLive(long timeToLive)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public long getTimeToLive()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setDeliveryDelay(long deliveryDelay)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public long getDeliveryDelay()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setAsync(CompletionListener completionListener)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public CompletionListener getAsync()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, boolean value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, byte value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, short value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, int value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, long value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, float value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, double value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, String value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setProperty(String name, Object value)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer clearProperties()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public boolean propertyExists(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public boolean getBooleanProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public byte getByteProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public short getShortProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public int getIntProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public long getLongProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public float getFloatProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public double getDoubleProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public String getStringProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public Object getObjectProperty(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public Set<String> getPropertyNames()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public byte[] getJMSCorrelationIDAsBytes()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setJMSCorrelationID(String correlationID)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public String getJMSCorrelationID()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setJMSType(String type)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public String getJMSType()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer setJMSReplyTo(Destination replyTo)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public Destination getJMSReplyTo()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }
}
