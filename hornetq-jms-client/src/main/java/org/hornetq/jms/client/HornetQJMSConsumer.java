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

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

/**
 * @author <a href="http://jmesnil.net/">Jeff Mesnil</a> (c) 2013 Red Hat inc.
 */
public class HornetQJMSConsumer implements JMSConsumer
{

   private final JMSContext context;
   private final MessageConsumer consumer;

   HornetQJMSConsumer(JMSContext context, MessageConsumer consumer)
   {
      this.context = context;
      this.consumer = consumer;
   }

   @Override
   public String getMessageSelector()
   {
      try
      {
         return consumer.getMessageSelector();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public MessageListener getMessageListener() throws JMSRuntimeException
   {
      try
      {
         return consumer.getMessageListener();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void setMessageListener(MessageListener listener) throws JMSRuntimeException
   {
      try
      {
         consumer.setMessageListener(listener);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public Message receive()
   {
      try
      {
         return consumer.receive();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public Message receive(long timeout)
   {
      try
      {
         return consumer.receive(timeout);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public Message receiveNoWait()
   {
      try
      {
         return consumer.receiveNoWait();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void close()
   {
      try
      {
         consumer.close();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public <T> T receiveBody(Class<T> c)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public <T> T receiveBody(Class<T> c, long timeout)
   {
      try
      {
         Message message = consumer.receive(timeout);
         // FIXME
         TextMessage textMessage = (TextMessage) message;
         return (T) textMessage.getText();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public <T> T receiveBodyNoWait(Class<T> c)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }
}
