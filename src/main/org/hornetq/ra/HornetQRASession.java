/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.ra;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.TransactionInProgressException;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;
import javax.resource.spi.ConnectionEvent;
import javax.transaction.xa.XAResource;

import org.hornetq.core.logging.Logger;

/**
 * A joint interface for JMS sessions
 * 
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class HornetQRASession implements Session, QueueSession, TopicSession, XASession, XAQueueSession, XATopicSession
{
   /** The logger */
   private static final Logger log = Logger.getLogger(HornetQRASession.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The managed connection */
   private HornetQRAManagedConnection mc;

   /** The connection request info */
   private final HornetQRAConnectionRequestInfo cri;

   /** The session factory */
   private HornetQRASessionFactory sf;

   /** The message consumers */
   private final Set consumers;

   /** The message producers */
   private final Set producers;

   /**
    * Constructor
    * @param mc The managed connection
    * @param cri The connection request info
    */
   public HornetQRASession(final HornetQRAManagedConnection mc, final HornetQRAConnectionRequestInfo cri)
   {
      if (trace)
      {
         log.trace("constructor(" + mc + ", " + cri + ")");
      }

      this.mc = mc;
      this.cri = cri;
      sf = null;
      consumers = new HashSet();
      producers = new HashSet();
   }

   /**
    * Set the session factory
    * @param sf The session factory
    */
   public void setHornetQSessionFactory(final HornetQRASessionFactory sf)
   {
      if (trace)
      {
         log.trace("setHornetQSessionFactory(" + sf + ")");
      }

      this.sf = sf;
   }

   /**
    * Lock
    * @exception JMSException Thrown if an error occurs
    * @exception IllegalStateException The session is closed
    */
   protected void lock() throws JMSException
   {
      if (trace)
      {
         log.trace("lock()");
      }

      HornetQRAManagedConnection mc = this.mc;
      if (mc != null)
      {
         mc.tryLock();
      }
      else
      {
         throw new IllegalStateException("Connection is not associated with a managed connection. " + this);
      }
   }

   /**
    * Unlock
    */
   protected void unlock()
   {
      if (trace)
      {
         log.trace("unlock()");
      }

      HornetQRAManagedConnection mc = this.mc;
      if (mc != null)
      {
         mc.unlock();
      }

      // We recreate the lock when returned to the pool
      // so missing the unlock after disassociation is not important
   }

   /**
    * Create a bytes message
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public BytesMessage createBytesMessage() throws JMSException
   {
      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createBytesMessage" + session);
      }

      return session.createBytesMessage();
   }

   /**
    * Create a map message
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public MapMessage createMapMessage() throws JMSException
   {
      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createMapMessage" + session);
      }

      return session.createMapMessage();
   }

   /**
    * Create a message
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public Message createMessage() throws JMSException
   {
      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createMessage" + session);
      }

      return session.createMessage();
   }

   /**
    * Create an object message
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public ObjectMessage createObjectMessage() throws JMSException
   {
      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createObjectMessage" + session);
      }

      return session.createObjectMessage();
   }

   /**
    * Create an object message
    * @param object The object
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public ObjectMessage createObjectMessage(final Serializable object) throws JMSException
   {
      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createObjectMessage(" + object + ")" + session);
      }

      return session.createObjectMessage(object);
   }

   /**
    * Create a stream message
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public StreamMessage createStreamMessage() throws JMSException
   {
      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createStreamMessage" + session);
      }

      return session.createStreamMessage();
   }

   /**
    * Create a text message
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public TextMessage createTextMessage() throws JMSException
   {
      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createTextMessage" + session);
      }

      return session.createTextMessage();
   }

   /**
    * Create a text message
    * @param string The text
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public TextMessage createTextMessage(final String string) throws JMSException
   {
      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createTextMessage(" + string + ")" + session);
      }

      return session.createTextMessage(string);
   }

   /**
    * Get transacted
    * @return True if transacted; otherwise false
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getTransacted() throws JMSException
   {
      if (trace)
      {
         log.trace("getTransacted()");
      }

      getSessionInternal();
      return cri.isTransacted();
   }

   /**
    * Get the message listener -- throws IllegalStateException
    * @return The message listener
    * @exception JMSException Thrown if an error occurs
    */
   public MessageListener getMessageListener() throws JMSException
   {
      if (trace)
      {
         log.trace("getMessageListener()");
      }

      throw new IllegalStateException("Method not allowed");
   }

   /**
    * Set the message listener -- Throws IllegalStateException
    * @param listener The message listener
    * @exception JMSException Thrown if an error occurs
    */
   public void setMessageListener(final MessageListener listener) throws JMSException
   {
      if (trace)
      {
         log.trace("setMessageListener(" + listener + ")");
      }

      throw new IllegalStateException("Method not allowed");
   }

   /**
    * Always throws an Error.
    * @exception Error Method not allowed.
    */
   public void run()
   {
      if (trace)
      {
         log.trace("run()");
      }

      throw new Error("Method not allowed");
   }

   /**
    * Closes the session. Sends a ConnectionEvent.CONNECTION_CLOSED to the
    * managed connection.
    * @exception JMSException Failed to close session.
    */
   public void close() throws JMSException
   {
      if (trace)
      {
         log.trace("close()");
      }

      sf.closeSession(this);
      closeSession();
   }

   /**
    * Commit
    * @exception JMSException Failed to close session.
    */
   public void commit() throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.XA_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_QUEUE_CONNECTION ||
          cri.getType() == HornetQRAConnectionFactory.XA_TOPIC_CONNECTION)
      {
         throw new TransactionInProgressException("XA connection");
      }

      lock();
      try
      {
         Session session = getSessionInternal();

         if (cri.isTransacted() == false)
         {
            throw new IllegalStateException("Session is not transacted");
         }

         if (trace)
         {
            log.trace("Commit session " + this);
         }

         session.commit();
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Rollback
    * @exception JMSException Failed to close session.
    */
   public void rollback() throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.XA_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_QUEUE_CONNECTION ||
          cri.getType() == HornetQRAConnectionFactory.XA_TOPIC_CONNECTION)
      {
         throw new TransactionInProgressException("XA connection");
      }

      lock();
      try
      {
         Session session = getSessionInternal();

         if (cri.isTransacted() == false)
         {
            throw new IllegalStateException("Session is not transacted");
         }

         if (trace)
         {
            log.trace("Rollback session " + this);
         }

         session.rollback();
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Recover
    * @exception JMSException Failed to close session.
    */
   public void recover() throws JMSException
   {
      lock();
      try
      {
         Session session = getSessionInternal();

         if (cri.isTransacted())
         {
            throw new IllegalStateException("Session is transacted");
         }

         if (trace)
         {
            log.trace("Recover session " + this);
         }

         session.recover();
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a topic
    * @param topicName The topic name
    * @return The topic
    * @exception JMSException Thrown if an error occurs
    */
   public Topic createTopic(final String topicName) throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_QUEUE_CONNECTION)
      {
         throw new IllegalStateException("Cannot create topic for javax.jms.QueueSession");
      }

      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createTopic " + session + " topicName=" + topicName);
      }

      Topic result = session.createTopic(topicName);

      if (trace)
      {
         log.trace("createdTopic " + session + " topic=" + result);
      }

      return result;
   }

   /**
    * Create a topic subscriber
    * @param topic The topic
    * @return The subscriber
    * @exception JMSException Thrown if an error occurs
    */
   public TopicSubscriber createSubscriber(final Topic topic) throws JMSException
   {
      lock();
      try
      {
         TopicSession session = getTopicSessionInternal();

         if (trace)
         {
            log.trace("createSubscriber " + session + " topic=" + topic);
         }

         TopicSubscriber result = session.createSubscriber(topic);
         result = new HornetQRATopicSubscriber(result, this);

         if (trace)
         {
            log.trace("createdSubscriber " + session + " HornetQTopicSubscriber=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a topic subscriber
    * @param topic The topic
    * @param messageSelector The message selector
    * @param noLocal If true inhibits the delivery of messages published by its own connection 
    * @return The subscriber
    * @exception JMSException Thrown if an error occurs
    */
   public TopicSubscriber createSubscriber(final Topic topic, final String messageSelector, final boolean noLocal) throws JMSException
   {
      lock();
      try
      {
         TopicSession session = getTopicSessionInternal();

         if (trace)
         {
            log.trace("createSubscriber " + session +
                      " topic=" +
                      topic +
                      " selector=" +
                      messageSelector +
                      " noLocal=" +
                      noLocal);
         }

         TopicSubscriber result = session.createSubscriber(topic, messageSelector, noLocal);
         result = new HornetQRATopicSubscriber(result, this);

         if (trace)
         {
            log.trace("createdSubscriber " + session + " HornetQTopicSubscriber=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a durable topic subscriber
    * @param topic The topic
    * @param name The name
    * @return The subscriber
    * @exception JMSException Thrown if an error occurs
    */
   public TopicSubscriber createDurableSubscriber(final Topic topic, final String name) throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_QUEUE_CONNECTION)
      {
         throw new IllegalStateException("Cannot create durable subscriber from javax.jms.QueueSession");
      }

      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("createDurableSubscriber " + session + " topic=" + topic + " name=" + name);
         }

         TopicSubscriber result = session.createDurableSubscriber(topic, name);
         result = new HornetQRATopicSubscriber(result, this);

         if (trace)
         {
            log.trace("createdDurableSubscriber " + session + " HornetQTopicSubscriber=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a topic subscriber
    * @param topic The topic
    * @param name The name
    * @param messageSelector The message selector
    * @param noLocal If true inhibits the delivery of messages published by its own connection 
    * @return The subscriber
    * @exception JMSException Thrown if an error occurs
    */
   public TopicSubscriber createDurableSubscriber(final Topic topic,
                                                  final String name,
                                                  final String messageSelector,
                                                  final boolean noLocal) throws JMSException
   {
      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("createDurableSubscriber " + session +
                      " topic=" +
                      topic +
                      " name=" +
                      name +
                      " selector=" +
                      messageSelector +
                      " noLocal=" +
                      noLocal);
         }

         TopicSubscriber result = session.createDurableSubscriber(topic, name, messageSelector, noLocal);
         result = new HornetQRATopicSubscriber(result, this);

         if (trace)
         {
            log.trace("createdDurableSubscriber " + session + " HornetQTopicSubscriber=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a topic publisher
    * @param topic The topic
    * @return The publisher
    * @exception JMSException Thrown if an error occurs
    */
   public TopicPublisher createPublisher(final Topic topic) throws JMSException
   {
      lock();
      try
      {
         TopicSession session = getTopicSessionInternal();

         if (trace)
         {
            log.trace("createPublisher " + session + " topic=" + topic);
         }

         TopicPublisher result = session.createPublisher(topic);
         result = new HornetQRATopicPublisher(result, this);

         if (trace)
         {
            log.trace("createdPublisher " + session + " publisher=" + result);
         }

         addProducer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a temporary topic
    * @return The temporary topic
    * @exception JMSException Thrown if an error occurs
    */
   public TemporaryTopic createTemporaryTopic() throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_QUEUE_CONNECTION)
      {
         throw new IllegalStateException("Cannot create temporary topic for javax.jms.QueueSession");
      }

      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("createTemporaryTopic " + session);
         }

         TemporaryTopic temp = session.createTemporaryTopic();

         if (trace)
         {
            log.trace("createdTemporaryTopic " + session + " temp=" + temp);
         }

         sf.addTemporaryTopic(temp);

         return temp;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Unsubscribe
    * @param name The name
    * @exception JMSException Thrown if an error occurs
    */
   public void unsubscribe(final String name) throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_QUEUE_CONNECTION)
      {
         throw new IllegalStateException("Cannot unsubscribe for javax.jms.QueueSession");
      }

      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("unsubscribe " + session + " name=" + name);
         }

         session.unsubscribe(name);
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a browser
    * @param queue The queue
    * @return The browser
    * @exception JMSException Thrown if an error occurs
    */
   public QueueBrowser createBrowser(final Queue queue) throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_TOPIC_CONNECTION)
      {
         throw new IllegalStateException("Cannot create browser for javax.jms.TopicSession");
      }

      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createBrowser " + session + " queue=" + queue);
      }

      QueueBrowser result = session.createBrowser(queue);

      if (trace)
      {
         log.trace("createdBrowser " + session + " browser=" + result);
      }

      return result;
   }

   /**
    * Create a browser
    * @param queue The queue
    * @param messageSelector The message selector
    * @return The browser
    * @exception JMSException Thrown if an error occurs
    */
   public QueueBrowser createBrowser(final Queue queue, final String messageSelector) throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_TOPIC_CONNECTION)
      {
         throw new IllegalStateException("Cannot create browser for javax.jms.TopicSession");
      }

      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createBrowser " + session + " queue=" + queue + " selector=" + messageSelector);
      }

      QueueBrowser result = session.createBrowser(queue, messageSelector);

      if (trace)
      {
         log.trace("createdBrowser " + session + " browser=" + result);
      }

      return result;
   }

   /**
    * Create a queue
    * @param queueName The queue name
    * @return The queue
    * @exception JMSException Thrown if an error occurs
    */
   public Queue createQueue(final String queueName) throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_TOPIC_CONNECTION)
      {
         throw new IllegalStateException("Cannot create browser or javax.jms.TopicSession");
      }

      Session session = getSessionInternal();

      if (trace)
      {
         log.trace("createQueue " + session + " queueName=" + queueName);
      }

      Queue result = session.createQueue(queueName);

      if (trace)
      {
         log.trace("createdQueue " + session + " queue=" + result);
      }

      return result;
   }

   /**
    * Create a queue receiver
    * @param queue The queue
    * @return The queue receiver
    * @exception JMSException Thrown if an error occurs
    */
   public QueueReceiver createReceiver(final Queue queue) throws JMSException
   {
      lock();
      try
      {
         QueueSession session = getQueueSessionInternal();

         if (trace)
         {
            log.trace("createReceiver " + session + " queue=" + queue);
         }

         QueueReceiver result = session.createReceiver(queue);
         result = new HornetQRAQueueReceiver(result, this);

         if (trace)
         {
            log.trace("createdReceiver " + session + " receiver=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a queue receiver
    * @param queue The queue
    * @param messageSelector
    * @return The queue receiver
    * @exception JMSException Thrown if an error occurs
    */
   public QueueReceiver createReceiver(final Queue queue, final String messageSelector) throws JMSException
   {
      lock();
      try
      {
         QueueSession session = getQueueSessionInternal();

         if (trace)
         {
            log.trace("createReceiver " + session + " queue=" + queue + " selector=" + messageSelector);
         }

         QueueReceiver result = session.createReceiver(queue, messageSelector);
         result = new HornetQRAQueueReceiver(result, this);

         if (trace)
         {
            log.trace("createdReceiver " + session + " receiver=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a queue sender
    * @param queue The queue
    * @return The queue sender
    * @exception JMSException Thrown if an error occurs
    */
   public QueueSender createSender(final Queue queue) throws JMSException
   {
      lock();
      try
      {
         QueueSession session = getQueueSessionInternal();

         if (trace)
         {
            log.trace("createSender " + session + " queue=" + queue);
         }

         QueueSender result = session.createSender(queue);
         result = new HornetQRAQueueSender(result, this);

         if (trace)
         {
            log.trace("createdSender " + session + " sender=" + result);
         }

         addProducer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a temporary queue
    * @return The temporary queue
    * @exception JMSException Thrown if an error occurs
    */
   public TemporaryQueue createTemporaryQueue() throws JMSException
   {
      if (cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION || cri.getType() == HornetQRAConnectionFactory.XA_TOPIC_CONNECTION)
      {
         throw new IllegalStateException("Cannot create temporary queue for javax.jms.TopicSession");
      }

      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("createTemporaryQueue " + session);
         }

         TemporaryQueue temp = session.createTemporaryQueue();

         if (trace)
         {
            log.trace("createdTemporaryQueue " + session + " temp=" + temp);
         }

         sf.addTemporaryQueue(temp);

         return temp;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a message consumer
    * @param destination The destination
    * @return The message consumer
    * @exception JMSException Thrown if an error occurs
    */
   public MessageConsumer createConsumer(final Destination destination) throws JMSException
   {
      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("createConsumer " + session + " dest=" + destination);
         }

         MessageConsumer result = session.createConsumer(destination);
         result = new HornetQRAMessageConsumer(result, this);

         if (trace)
         {
            log.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a message consumer
    * @param destination The destination
    * @param messageSelector The message selector
    * @return The message consumer
    * @exception JMSException Thrown if an error occurs
    */
   public MessageConsumer createConsumer(final Destination destination, final String messageSelector) throws JMSException
   {
      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("createConsumer " + session + " dest=" + destination + " messageSelector=" + messageSelector);
         }

         MessageConsumer result = session.createConsumer(destination, messageSelector);
         result = new HornetQRAMessageConsumer(result, this);

         if (trace)
         {
            log.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a message consumer
    * @param destination The destination
    * @param messageSelector The message selector
    * @param noLocal If true inhibits the delivery of messages published by its own connection 
    * @return The message consumer
    * @exception JMSException Thrown if an error occurs
    */
   public MessageConsumer createConsumer(final Destination destination,
                                         final String messageSelector,
                                         final boolean noLocal) throws JMSException
   {
      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("createConsumer " + session +
                      " dest=" +
                      destination +
                      " messageSelector=" +
                      messageSelector +
                      " noLocal=" +
                      noLocal);
         }

         MessageConsumer result = session.createConsumer(destination, messageSelector, noLocal);
         result = new HornetQRAMessageConsumer(result, this);

         if (trace)
         {
            log.trace("createdConsumer " + session + " consumer=" + result);
         }

         addConsumer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Create a message producer
    * @param destination The destination
    * @return The message producer
    * @exception JMSException Thrown if an error occurs
    */
   public MessageProducer createProducer(final Destination destination) throws JMSException
   {
      lock();
      try
      {
         Session session = getSessionInternal();

         if (trace)
         {
            log.trace("createProducer " + session + " dest=" + destination);
         }

         MessageProducer result = session.createProducer(destination);
         result = new HornetQRAMessageProducer(result, this);

         if (trace)
         {
            log.trace("createdProducer " + session + " producer=" + result);
         }

         addProducer(result);

         return result;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Get the acknowledge mode
    * @return The mode
    * @exception JMSException Thrown if an error occurs
    */
   public int getAcknowledgeMode() throws JMSException
   {
      if (trace)
      {
         log.trace("getAcknowledgeMode()");
      }

      getSessionInternal();
      return cri.getAcknowledgeMode();
   }

   /**
    * Get the XA resource
    * @return The XA resource
    * @exception IllegalStateException If non XA connection
    */
   public XAResource getXAResource()
   {
      if (trace)
      {
         log.trace("getXAResource()");
      }

      if (cri.getType() == HornetQRAConnectionFactory.CONNECTION || cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION ||
          cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION)
      {
         return null;
      }

      try
      {
         lock();

         XASession session = getXASessionInternal();

         return session.getXAResource();
      }
      catch (Throwable t)
      {
         return null;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Get the session
    * @return The session
    * @exception JMSException Thrown if an error occurs
    */
   public Session getSession() throws JMSException
   {
      if (trace)
      {
         log.trace("getSession()");
      }

      if (cri.getType() == HornetQRAConnectionFactory.CONNECTION || cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION ||
          cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION)
      {
         throw new IllegalStateException("Non XA connection");
      }

      lock();
      try
      {
         return this;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Get the queue session
    * @return The queue session
    * @exception JMSException Thrown if an error occurs
    */
   public QueueSession getQueueSession() throws JMSException
   {
      if (trace)
      {
         log.trace("getQueueSession()");
      }

      if (cri.getType() == HornetQRAConnectionFactory.CONNECTION || cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION ||
          cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION)
      {
         throw new IllegalStateException("Non XA connection");
      }

      lock();
      try
      {
         return this;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Get the topic session
    * @return The topic session
    * @exception JMSException Thrown if an error occurs
    */
   public TopicSession getTopicSession() throws JMSException
   {
      if (trace)
      {
         log.trace("getTopicSession()");
      }

      if (cri.getType() == HornetQRAConnectionFactory.CONNECTION || cri.getType() == HornetQRAConnectionFactory.QUEUE_CONNECTION ||
          cri.getType() == HornetQRAConnectionFactory.TOPIC_CONNECTION)
      {
         throw new IllegalStateException("Non XA connection");
      }

      lock();
      try
      {
         return this;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Set the managed connection
    * @param managedConnection The managed connection
    */
   void setManagedConnection(final HornetQRAManagedConnection managedConnection)
   {
      if (trace)
      {
         log.trace("setManagedConnection(" + managedConnection + ")");
      }

      if (mc != null)
      {
         mc.removeHandle(this);
      }

      mc = managedConnection;
   }

   /**
    * Destroy
    */
   void destroy()
   {
      if (trace)
      {
         log.trace("destroy()");
      }

      mc = null;
   }

   /**
    * Start
    * @exception JMSException Thrown if an error occurs
    */
   void start() throws JMSException
   {
      if (trace)
      {
         log.trace("start()");
      }

      if (mc != null)
      {
         mc.start();
      }
   }

   /**
    * Stop
    * @exception JMSException Thrown if an error occurs
    */
   void stop() throws JMSException
   {
      if (trace)
      {
         log.trace("stop()");
      }

      if (mc != null)
      {
         mc.stop();
      }
   }

   /**
    * Check strict
    * @exception JMSException Thrown if an error occurs
    */
   void checkStrict() throws JMSException
   {
      if (trace)
      {
         log.trace("checkStrict()");
      }

      if (mc != null)
      {
         throw new IllegalStateException(HornetQRASessionFactory.ISE);
      }
   }

   /**
    * Close session
    * @exception JMSException Thrown if an error occurs
    */
   void closeSession() throws JMSException
   {
      if (mc != null)
      {
         log.trace("Closing session");

         try
         {
            mc.stop();
         }
         catch (Throwable t)
         {
            log.trace("Error stopping managed connection", t);
         }

         synchronized (consumers)
         {
            for (Iterator i = consumers.iterator(); i.hasNext();)
            {
               HornetQRAMessageConsumer consumer = (HornetQRAMessageConsumer)i.next();
               try
               {
                  consumer.closeConsumer();
               }
               catch (Throwable t)
               {
                  log.trace("Error closing consumer", t);
               }
               i.remove();
            }
         }

         synchronized (producers)
         {
            for (Iterator i = producers.iterator(); i.hasNext();)
            {
               HornetQRAMessageProducer producer = (HornetQRAMessageProducer)i.next();
               try
               {
                  producer.closeProducer();
               }
               catch (Throwable t)
               {
                  log.trace("Error closing producer", t);
               }
               i.remove();
            }
         }

         mc.removeHandle(this);
         ConnectionEvent ev = new ConnectionEvent(mc, ConnectionEvent.CONNECTION_CLOSED);
         ev.setConnectionHandle(this);
         mc.sendEvent(ev);
         mc = null;
      }
   }

   /**
    * Add consumer
    * @param consumer The consumer
    */
   void addConsumer(final MessageConsumer consumer)
   {
      if (trace)
      {
         log.trace("addConsumer(" + consumer + ")");
      }

      synchronized (consumers)
      {
         consumers.add(consumer);
      }
   }

   /**
    * Remove consumer
    * @param consumer The consumer
    */
   void removeConsumer(final MessageConsumer consumer)
   {
      if (trace)
      {
         log.trace("removeConsumer(" + consumer + ")");
      }

      synchronized (consumers)
      {
         consumers.remove(consumer);
      }
   }

   /**
    * Add producer
    * @param producer The producer
    */
   void addProducer(final MessageProducer producer)
   {
      if (trace)
      {
         log.trace("addProducer(" + producer + ")");
      }

      synchronized (producers)
      {
         producers.add(producer);
      }
   }

   /**
    * Remove producer
    * @param producer The producer
    */
   void removeProducer(final MessageProducer producer)
   {
      if (trace)
      {
         log.trace("removeProducer(" + producer + ")");
      }

      synchronized (producers)
      {
         producers.remove(producer);
      }
   }

   /**
    * Get the session and ensure that it is open
    * @return The session
    * @exception JMSException Thrown if an error occurs
    * @exception IllegalStateException The session is closed
    */
   Session getSessionInternal() throws JMSException
   {
      if (mc == null)
      {
         throw new IllegalStateException("The session is closed");
      }

      Session session = mc.getSession();

      if (trace)
      {
         log.trace("getSessionInternal " + session + " for " + this);
      }

      return session;
   }

   /**
    * Get the XA session and ensure that it is open
    * @return The session
    * @exception JMSException Thrown if an error occurs
    * @exception IllegalStateException The session is closed
    */
   XASession getXASessionInternal() throws JMSException
   {
      if (mc == null)
      {
         throw new IllegalStateException("The session is closed");
      }

      XASession session = mc.getXASession();

      if (trace)
      {
         log.trace("getXASessionInternal " + session + " for " + this);
      }

      return session;
   }

   /**
    * Get the queue session
    * @return The queue session
    * @exception JMSException Thrown if an error occurs
    * @exception IllegalStateException The session is closed
    */
   QueueSession getQueueSessionInternal() throws JMSException
   {
      Session s = getSessionInternal();
      if (!(s instanceof QueueSession))
      {
         throw new InvalidDestinationException("Attempting to use QueueSession methods on: " + this);
      }
      return (QueueSession)s;
   }

   /**
    * Get the topic session
    * @return The topic session
    * @exception JMSException Thrown if an error occurs
    * @exception IllegalStateException The session is closed
    */
   TopicSession getTopicSessionInternal() throws JMSException
   {
      Session s = getSessionInternal();
      if (!(s instanceof TopicSession))
      {
         throw new InvalidDestinationException("Attempting to use TopicSession methods on: " + this);
      }
      return (TopicSession)s;
   }
}
