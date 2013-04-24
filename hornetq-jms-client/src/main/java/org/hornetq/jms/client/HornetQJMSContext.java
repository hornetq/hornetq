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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

/**
 * HornetQ implementation of a JMSContext.
 *
 * @author <a href="http://jmesnil.net/">Jeff Mesnil</a> (c) 2013 Red Hat inc
 */
public class HornetQJMSContext implements JMSContext
{
   private final ConnectionFactory cf;
   private final int ackMode;
   private final String userName;
   private final String password;

   private final Connection connection;
   private final Session session;

   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQJMSContext(ConnectionFactory cf, int ackMode, String userName, String password)
   {
      this.cf = cf;
      this.ackMode = ackMode;
      this.userName = userName;
      this.password = password;

      try
      {
         connection = cf.createConnection(userName, password);
         session = connection.createSession(ackMode);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   // JMSContext implementation -------------------------------------

   @Override
   public JMSContext createContext(int sessionMode)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSProducer createProducer()
   {
      try
      {
         return new HornetQJMSProducer(this, session.createProducer(null));
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public String getClientID()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void setClientID(String clientID)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public ConnectionMetaData getMetaData()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public ExceptionListener getExceptionListener()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void setExceptionListener(ExceptionListener listener)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void start()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void stop()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void setAutoStart(boolean autoStart)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public boolean getAutoStart()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void close()
   {
      try
      {
         session.close();
         connection.close();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public BytesMessage createBytesMessage()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public MapMessage createMapMessage()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public Message createMessage()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public ObjectMessage createObjectMessage()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public ObjectMessage createObjectMessage(Serializable object)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public StreamMessage createStreamMessage()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public TextMessage createTextMessage()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public TextMessage createTextMessage(String text)
   {
      try
      {
         return session.createTextMessage(text);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public boolean getTransacted()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public int getSessionMode()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void commit()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void rollback()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void recover()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSConsumer createConsumer(Destination destination)
   {
      try
      {
         return new HornetQJMSConsumer(this, session.createConsumer(destination));
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createConsumer(Destination destination, String messageSelector)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public Queue createQueue(String queueName)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public Topic createTopic(String topicName)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSConsumer createDurableConsumer(Topic topic, String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSConsumer createSharedDurableConsumer(Topic topic, String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public QueueBrowser createBrowser(Queue queue)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public QueueBrowser createBrowser(Queue queue, String messageSelector)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public TemporaryQueue createTemporaryQueue()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public TemporaryTopic createTemporaryTopic()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void unsubscribe(String name)
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   @Override
   public void acknowledge()
   {
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
