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
   private static final boolean DEFAULT_AUTO_START = true;
   private final ConnectionFactory cf;
   private final int ackMode;
   private final String userName;
   private final String password;

   private final HornetQConnection connection;
   private final HornetQSession session;
   private boolean autoStart = HornetQJMSContext.DEFAULT_AUTO_START;

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
         connection = (HornetQConnection) cf.createConnection(userName, password);
         session = (HornetQSession) connection.createSession(ackMode);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   // JMSContext implementation -------------------------------------

   @Override
   public JMSContext createContext(int sessionMode)
   {
      return new HornetQJMSContext(cf, sessionMode, userName, password);
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
      try
      {
         return connection.getClientID();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void setClientID(String clientID)
   {
      try
      {
         connection.setClientID(clientID);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public ConnectionMetaData getMetaData()
   {
      try
      {
         return connection.getMetaData();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public ExceptionListener getExceptionListener()
   {
      try
      {
         return connection.getExceptionListener();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void setExceptionListener(ExceptionListener listener)
   {
      try
      {
         connection.setExceptionListener(listener);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void start()
   {
      try
      {
         connection.start();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void stop()
   {
      try
      {
         connection.stop();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void setAutoStart(boolean autoStart)
   {
      this.autoStart = autoStart;
   }

   @Override
   public boolean getAutoStart()
   {
      return autoStart;
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
      try
      {
         return session.createBytesMessage();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public MapMessage createMapMessage()
   {
      try
      {
         return session.createMapMessage();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public Message createMessage()
   {
      try
      {
         return session.createMessage();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public ObjectMessage createObjectMessage()
   {
      try
      {
         return session.createObjectMessage();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public ObjectMessage createObjectMessage(Serializable object)
   {
      try
      {
         return session.createObjectMessage(object);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public StreamMessage createStreamMessage()
   {
      try
      {
         return session.createStreamMessage();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public TextMessage createTextMessage()
   {
      try
      {
         return session.createTextMessage();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
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
      try
      {
         return session.getTransacted();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public int getSessionMode()
   {
      return ackMode;
   }

   @Override
   public void commit()
   {
      try
      {
         session.commit();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void rollback()
   {
      try
      {
         session.rollback();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void recover()
   {
      try
      {
         session.recover();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createConsumer(Destination destination)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createConsumer(destination));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createConsumer(Destination destination, String messageSelector)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createConsumer(destination, messageSelector));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createConsumer(destination, messageSelector, noLocal));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public Queue createQueue(String queueName)
   {
      try
      {
         return session.createQueue(queueName);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public Topic createTopic(String topicName)
   {
      try
      {
         return session.createTopic(topicName);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createDurableConsumer(Topic topic, String name)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createDurableConsumer(topic, name));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createDurableConsumer(topic, name, messageSelector, noLocal));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createSharedDurableConsumer(Topic topic, String name)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createSharedDurableConsumer(topic, name));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createSharedDurableConsumer(topic, name, messageSelector));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createSharedConsumer(topic, sharedSubscriptionName));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector)
   {
      try
      {
         HornetQJMSConsumer consumer = new HornetQJMSConsumer(this, session.createSharedConsumer(topic, sharedSubscriptionName, messageSelector));
         checkAutoStart();
         return consumer;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public QueueBrowser createBrowser(Queue queue)
   {
      try
      {
         QueueBrowser browser = session.createBrowser(queue);
         checkAutoStart();
         return browser;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public QueueBrowser createBrowser(Queue queue, String messageSelector)
   {
      try
      {
         QueueBrowser browser = session.createBrowser(queue, messageSelector);
         checkAutoStart();
         return browser;
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public TemporaryQueue createTemporaryQueue()
   {
      try
      {
         return session.createTemporaryQueue();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public TemporaryTopic createTemporaryTopic()
   {
      try
      {
         return session.createTemporaryTopic();
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void unsubscribe(String name)
   {
      try
      {
         session.unsubscribe(name);
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public void acknowledge()
   {
      //todo add ack on session for all consumers
      throw new UnsupportedOperationException("JMS 2.0 / not implemented");
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private synchronized void checkAutoStart() throws JMSException
   {
      if(autoStart && !connection.isStarted())
      {
         connection.start();
      }
   }
   // Inner classes -------------------------------------------------

}
