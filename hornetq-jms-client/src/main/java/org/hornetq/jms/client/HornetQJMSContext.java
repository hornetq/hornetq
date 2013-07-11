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
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateRuntimeException;
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
   private final int ackMode;

   private final HornetQConnectionForContext connection;
   private Session session;
   private boolean autoStart = HornetQJMSContext.DEFAULT_AUTO_START;
   private boolean closed;


   public HornetQJMSContext(HornetQConnectionForContext connection, int ackMode)
   {
      this.connection = connection;
      this.ackMode = ackMode;
   }

   // JMSContext implementation -------------------------------------

   @Override
   public JMSContext createContext(int sessionMode)
   {
      return connection.createContext(sessionMode);
   }

   @Override
   public JMSProducer createProducer()
   {
      checkSession();
      try
      {
         return new HornetQJMSProducer(this, session.createProducer(null));
      } catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   /**
    *
    */
   private void checkSession()
   {
      if (session == null)
      {
         synchronized (this)
         {
            if (closed)
               throw new IllegalStateRuntimeException("Context is closed");
            if (session == null)
            {
               try
               {
                  session = connection.createSession(ackMode);
               }
               catch (JMSException e)
               {
                  throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
               }
            }
         }
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
         synchronized (this)
         {
            if (session != null)
               session.close();
            connection.closeFromContext();
            closed = true;
         }
      }
      catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   @Override
   public BytesMessage createBytesMessage()
   {
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
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
      checkSession();
      if (closed)
         throw new IllegalStateRuntimeException("Context is closed");
      try
      {
         if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
            return;
         session.commit();
      }
      catch (JMSException e)
      {
         throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
   }

   private synchronized void checkAutoStart() throws JMSException
   {
      if (closed)
         throw new IllegalStateRuntimeException("Context is closed");
      if (autoStart)
      {
         connection.start();
      }
   }
}
