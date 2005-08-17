/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.logging.Logger;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.destination.JBossTemporaryQueue;
import org.jboss.jms.destination.JBossTemporaryTopic;
import org.jboss.jms.server.container.JMSAdvisor;

import javax.jms.InvalidDestinationException;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.QueueBrowser;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.XAQueueSession;
import javax.jms.XASession;
import javax.jms.XATopicSession;
import javax.transaction.xa.XAResource;

import javax.jms.IllegalStateException;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
class JBossSession implements
   Session, XASession, QueueSession, XAQueueSession,
   TopicSession, XATopicSession, Serializable
{
   // Constants -----------------------------------------------------
   static final int TYPE_GENERIC_SESSION = 0;
   static final int TYPE_QUEUE_SESSION = 1;
   static final int TYPE_TOPIC_SESSION = 2;
   
   private static final Logger log = Logger.getLogger(JBossSession.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected SessionDelegate sessionDelegate;
   protected boolean isXA;
   protected int sessionType;
   protected int acknowledgeMode;

   // Constructors --------------------------------------------------

   public JBossSession(SessionDelegate sessionDelegate,
                       boolean isXA,
                       int sessionType,
                       boolean transacted,
                       int acknowledgeMode) throws JMSException
   {
      this.sessionDelegate = sessionDelegate;      
      this.isXA = isXA;
      this.sessionType = sessionType;
      sessionDelegate.addMetaData(JMSAdvisor.TRANSACTED, transacted ? Boolean.TRUE : Boolean.FALSE);
      this.acknowledgeMode = acknowledgeMode;
   }

   // Session implementation ----------------------------------------

   public BytesMessage createBytesMessage() throws JMSException
   {
   	return sessionDelegate.createBytesMessage();
   }

   public MapMessage createMapMessage() throws JMSException
   {
   	return sessionDelegate.createMapMessage();
   }

   public Message createMessage() throws JMSException
   {
      return sessionDelegate.createMessage();
   }

   public ObjectMessage createObjectMessage() throws JMSException
   {
   	return sessionDelegate.createObjectMessage();
   }

   public ObjectMessage createObjectMessage(Serializable object) throws JMSException
   {
   	return sessionDelegate.createObjectMessage(object);
   }

   public StreamMessage createStreamMessage() throws JMSException
   {
   	return sessionDelegate.createStreamMessage();
   }

   public TextMessage createTextMessage() throws JMSException
   {
   	return sessionDelegate.createTextMessage();
   }

   public TextMessage createTextMessage(String text) throws JMSException
   {
   	return sessionDelegate.createTextMessage(text);
   }

   public boolean getTransacted() throws JMSException
   {
      return ((Boolean)sessionDelegate.getMetaData(JMSAdvisor.TRANSACTED)).booleanValue();
   }

   public int getAcknowledgeMode() throws JMSException
   {
      return acknowledgeMode;
   }

   public void commit() throws JMSException
   {
      sessionDelegate.commit();
   }

   public void rollback() throws JMSException
   {
      sessionDelegate.rollback();
   }

   public void close() throws JMSException
   {
      sessionDelegate.closing();
      sessionDelegate.close();
   }

   public void recover() throws JMSException
   {
      sessionDelegate.recover();
   }

   public MessageListener getMessageListener() throws JMSException
   {
      //TODO - This is optional - not required for basic JMS1.1 compliance
      throw new NotYetImplementedException();
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
      //TODO - This is optional - not required for basic JMS1.1 compliance
      throw new NotYetImplementedException();
   }

   public void run()
   {
      //TODO - This is optional - not required for basic JMS1.1 compliance
      throw new NotYetImplementedException();
   }

   public MessageProducer createProducer(Destination d) throws JMSException
   {
      ProducerDelegate producerDelegate = sessionDelegate.createProducerDelegate(d);
      return new JBossMessageProducer(producerDelegate, d);
   }

  public MessageConsumer createConsumer(Destination d) throws JMSException
  {
     if (d == null)
     {
        throw new InvalidDestinationException("Cannot create a consumer with a null destination");
     }
     ConsumerDelegate consumerDelegate =
           sessionDelegate.createConsumerDelegate(d, null, false, null);
     return new JBossMessageConsumer(consumerDelegate, false);
  }

  public MessageConsumer createConsumer(Destination d, String messageSelector) throws JMSException
  {
     if (log.isTraceEnabled()) { log.trace("Attempting to create consumer for destination:" + d +
           ", messageSelector: " + messageSelector); }
     if (d == null)
     {
        throw new InvalidDestinationException("Cannot create a consumer with a null destination");
     }
	  ConsumerDelegate consumerDelegate =
        sessionDelegate.createConsumerDelegate(d, messageSelector, false, null);
     return new JBossMessageConsumer(consumerDelegate, false);
  }

   public MessageConsumer createConsumer(Destination d,
                                         String messageSelector,
                                         boolean noLocal)
         throws JMSException
   {
      if (log.isTraceEnabled()) { log.trace("Attempting to create consumer for destination:" + d +
            ", messageSelector: " + messageSelector + ", noLocal: " + noLocal); }
      
      if (d == null)
      {
         throw new InvalidDestinationException("Cannot create a consumer with a null destination");
      }
      ConsumerDelegate consumerDelegate =
         sessionDelegate.createConsumerDelegate(d, messageSelector, noLocal, null);
      return new JBossMessageConsumer(consumerDelegate, noLocal);
   }

   public Queue createQueue(String queueName) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a queue using a TopicSession");
      }
      return sessionDelegate.createQueue(queueName);
   }

   public Topic createTopic(String topicName) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a topic on a QueueSession");
      }
      return sessionDelegate.createTopic(topicName);
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
      ConsumerDelegate consumerDelegate =
            sessionDelegate.createConsumerDelegate(topic, null, false, name);
      return new JBossMessageConsumer(consumerDelegate, false);
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
      if ("".equals(messageSelector))
      {
         messageSelector = null;
      }
      ConsumerDelegate consumerDelegate =
         sessionDelegate.createConsumerDelegate(topic, messageSelector, noLocal, name);
      return new JBossMessageConsumer(consumerDelegate, noLocal);
   }

   public QueueBrowser createBrowser(Queue queue) throws JMSException
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
      if ("".equals(messageSelector))
      {
         messageSelector = null;
      }
      BrowserDelegate delegate = this.sessionDelegate.createBrowserDelegate(queue, messageSelector);
      return new JBossQueueBrowser(queue, messageSelector, delegate);
   }

   public TemporaryQueue createTemporaryQueue() throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a temp. queue using a TopicSession");
      }
      JBossTemporaryQueue queue = new JBossTemporaryQueue(sessionDelegate);
      sessionDelegate.addTemporaryDestination(queue);
      return queue;
   }

   public TemporaryTopic createTemporaryTopic() throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a temporary topic on a QueueSession");
      }
      JBossTemporaryTopic topic = new JBossTemporaryTopic(sessionDelegate);
      sessionDelegate.addTemporaryDestination(topic);
      return topic;
   }

   public void unsubscribe(String name) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot unsubscribe using a QueueSession");
      }
      sessionDelegate.unsubscribe(name);
   }
   
   // XASession implementation
   
   public Session getSession() throws JMSException
   {
      if (!isXA)
      {
         throw new IllegalStateException("Is not an XASession");
      }
      return this;
   }
  
   public XAResource getXAResource()
   {          
      // TODO -  not required for basic JMS1.1 compliance
      throw new NotYetImplementedException();
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

   // Package protected ---------------------------------------------
      
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
    
}
