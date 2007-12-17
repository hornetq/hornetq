/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.client;

import java.io.Serializable;

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
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;
import javax.transaction.xa.XAResource;

import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTemporaryQueue;
import org.jboss.jms.destination.JBossTemporaryTopic;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class JBossSession implements
   Session, XASession, QueueSession, XAQueueSession,
   TopicSession, XATopicSession, Serializable
{   
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 2235942510476264909L;
   
   static final int TYPE_GENERIC_SESSION = 0;
   
   static final int TYPE_QUEUE_SESSION = 1;
   
   static final int TYPE_TOPIC_SESSION = 2;

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JBossSession.class);
   
   // Attributes ----------------------------------------------------
   
   protected SessionDelegate delegate;

   protected int sessionType;

   // Constructors --------------------------------------------------

   public JBossSession(SessionDelegate sessionDelegate, int sessionType)
   {
      this.delegate = sessionDelegate;
      this.sessionType = sessionType;
   }

   // Session implementation ----------------------------------------

   public BytesMessage createBytesMessage() throws JMSException
   {
   	return delegate.createBytesMessage();
   }

   public MapMessage createMapMessage() throws JMSException
   {
   	return delegate.createMapMessage();
   }

   public Message createMessage() throws JMSException
   {
      return delegate.createMessage();
   }

   public ObjectMessage createObjectMessage() throws JMSException
   {
   	return delegate.createObjectMessage();
   }

   public ObjectMessage createObjectMessage(Serializable object) throws JMSException
   {
   	return delegate.createObjectMessage(object);
   }

   public StreamMessage createStreamMessage() throws JMSException
   {
   	return delegate.createStreamMessage();
   }

   public TextMessage createTextMessage() throws JMSException
   {
   	return delegate.createTextMessage();
   }

   public TextMessage createTextMessage(String text) throws JMSException
   {
   	return delegate.createTextMessage(text);
   }

   public boolean getTransacted() throws JMSException
   {
      return delegate.getTransacted();
   }

   public int getAcknowledgeMode() throws JMSException
   {
      return delegate.getAcknowledgeMode();
   }

   public void commit() throws JMSException
   {
      delegate.commit();
   }

   public void rollback() throws JMSException
   {
      delegate.rollback();
   }

   public void close() throws JMSException
   {
      delegate.closing(-1);
      delegate.close();
   }

   public void recover() throws JMSException
   {
      delegate.recover();
   }

   public MessageListener getMessageListener() throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("getMessageListener() called"); }
      return delegate.getMessageListener();
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("setMessageListener(" + listener + ") called"); }

      delegate.setMessageListener(listener);
   }

   public void run()
   {
      if (log.isTraceEnabled()) { log.trace("run() called"); }
      delegate.run();
   }

   public MessageProducer createProducer(Destination d) throws JMSException
   {
      if (d != null && !(d instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Not a JBossDestination:" + d);
      }
           
      ProducerDelegate producerDelegate = delegate.createProducerDelegate((JBossDestination)d);
      return new JBossMessageProducer(producerDelegate);
   }

  public MessageConsumer createConsumer(Destination d) throws JMSException
  {
     return createConsumer(d, null, false);
  }

  public MessageConsumer createConsumer(Destination d, String messageSelector) throws JMSException
  {
     return createConsumer(d, messageSelector, false);
  }

   public MessageConsumer createConsumer(Destination d, String messageSelector, boolean noLocal)
         throws JMSException
   {
      if (d == null)
      {
         throw new InvalidDestinationException("Cannot create a consumer with a null destination");
      }
      if (!(d instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Not a JBossDestination:" + d);
      }

      log.trace("attempting to create consumer for destination:" + d + (messageSelector == null ? "" : ", messageSelector: " + messageSelector) + (noLocal ? ", noLocal = true" : ""));

      ConsumerDelegate cd = delegate.
         createConsumerDelegate((JBossDestination)d, messageSelector, noLocal, null, false, true);

      return new JBossMessageConsumer(cd);
   }

   public Queue createQueue(String queueName) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a queue using a TopicSession");
      }
      return delegate.createQueue(queueName);
   }

   public Topic createTopic(String topicName) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a topic on a QueueSession");
      }
      return delegate.createTopic(topicName);
   }

   public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a durable subscriber on a QueueSession");
      }
      if (topic == null)
      {
         throw new InvalidDestinationException("Cannot create a durable subscriber on a null topic");
      }
      if (!(topic instanceof JBossTopic))
      {
         throw new InvalidDestinationException("Not a JBossTopic:" + topic);
      }

      ConsumerDelegate cd =
         delegate.createConsumerDelegate((JBossTopic)topic, null, false, name, false, true);

      return new JBossMessageConsumer(cd);
   }

   public TopicSubscriber createDurableSubscriber(Topic topic,
                                                  String name,
                                                  String messageSelector,
                                                  boolean noLocal)
         throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a durable subscriber on a QueueSession");
      }
      if (topic == null)
      {
         throw new InvalidDestinationException("Cannot create a durable subscriber on a null topic");
      }
      if (!(topic instanceof JBossTopic))
      {
         throw new InvalidDestinationException("Not a JBossTopic:" + topic);
      }
      if ("".equals(messageSelector))
      {
         messageSelector = null;
      }

      ConsumerDelegate cd = delegate.
         createConsumerDelegate((JBossTopic)topic, messageSelector, noLocal, name, false, true);

      return new JBossMessageConsumer(cd);
   }

   public QueueBrowser createBrowser(Queue queue) throws JMSException
   {
      return createBrowser(queue, null);
   }

   public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a browser on a TopicSession");
      }
      if (queue == null)
      {
         throw new InvalidDestinationException("Cannot create a browser with a null queue");
      }
      if (!(queue instanceof JBossQueue))
      {
         throw new InvalidDestinationException("Not a JBossQueue:" + queue);
      }
      if ("".equals(messageSelector))
      {
         messageSelector = null;
      }

      BrowserDelegate del =
         delegate.createBrowserDelegate((JBossQueue)queue, messageSelector);

      return new JBossQueueBrowser(queue, messageSelector, del);
   }

   public TemporaryQueue createTemporaryQueue() throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a temporary queue using a TopicSession");
      }
      JBossTemporaryQueue queue = new JBossTemporaryQueue(delegate);
      delegate.addTemporaryDestination(queue);
      return queue;
   }

   public TemporaryTopic createTemporaryTopic() throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a temporary topic on a QueueSession");
      }
      JBossTemporaryTopic topic = new JBossTemporaryTopic(delegate);
      delegate.addTemporaryDestination(topic);
      return topic;
   }

   public void unsubscribe(String name) throws JMSException
   {
      // As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot unsubscribe using a QueueSession");
      }
      delegate.unsubscribe(name);
   }
   
   // XASession implementation
   
   public Session getSession() throws JMSException
   {      
      SessionState state = (SessionState)((DelegateSupport)delegate).getState();
      if (!state.isXA())
      {
         throw new IllegalStateException("Isn't an XASession");
      }
      
      return this;
   }
  
   public XAResource getXAResource()
   {          
      return delegate.getXAResource();
   }
   
   // QueueSession implementation
   
   public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException
   {
      return (QueueReceiver)createConsumer(queue, messageSelector);
   }

   public QueueReceiver createReceiver(Queue queue) throws JMSException
   {
      return (QueueReceiver)createConsumer(queue);
   }

   public QueueSender createSender(Queue queue) throws JMSException
   {
      return (QueueSender)createProducer(queue);
   }
   
   // XAQueueSession implementation
   
   public QueueSession getQueueSession() throws JMSException
   {
      return (QueueSession)getSession();
   }
   
   // TopicSession implementation
   
   public TopicPublisher createPublisher(Topic topic) throws JMSException
   {
      return (TopicPublisher)createProducer(topic);
   }

   public TopicSubscriber createSubscriber(Topic topic, String messageSelector,
         boolean noLocal) throws JMSException
   {
      return (TopicSubscriber)createConsumer(topic, messageSelector, noLocal);
   }

   public TopicSubscriber createSubscriber(Topic topic) throws JMSException
   {
      return (TopicSubscriber)createConsumer(topic);
   }
   
   // XATopicSession implementation
   
   public TopicSession getTopicSession() throws JMSException
   {
      return (TopicSession)getSession();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "JBossSession->" + delegate;
   }
   
   public SessionDelegate getDelegate()
   {
      return delegate;
   }

   // Package protected ---------------------------------------------
   
   /*
    * This method is used by the JBossConnectionConsumer to load up the session
    * with messages to be processed by the session's run() method
    */
   void addAsfMessage(JBossMessage m, String consumerID, String queueName, int maxDeliveries,
                      SessionDelegate connectionConsumerSession, boolean shouldAck)
   {
      delegate.addAsfMessage(m, consumerID, queueName, maxDeliveries, connectionConsumerSession, shouldAck);
   }
      
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
    
}
