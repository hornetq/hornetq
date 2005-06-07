/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.BrowserDelegate;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.destination.JBossTemporaryQueue;
import org.jboss.jms.destination.JBossTemporaryTopic;

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
class JBossSession
      implements Session, XASession, QueueSession, XAQueueSession, TopicSession, XATopicSession
{
   // Constants -----------------------------------------------------
   static final int TYPE_GENERIC_SESSION = 0;
   static final int TYPE_QUEUE_SESSION = 1;
   static final int TYPE_TOPIC_SESSION = 2;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected SessionDelegate sessionDelegate;
   protected ConnectionDelegate connectionDelegate;
   protected boolean isXA;
   protected int sessionType;
   protected boolean transacted;
   protected int acknowledgeMode;   

   // Constructors --------------------------------------------------

   public JBossSession(ConnectionDelegate connectionDelegate,
                       SessionDelegate sessionDelegate,
                       boolean isXA,
                       int sessionType,
                       boolean transacted,
                       int acknowledgeMode)
   {
      this.sessionDelegate = sessionDelegate;
      this.connectionDelegate = connectionDelegate;
      this.isXA = isXA;
      this.sessionType = sessionType;
      this.transacted = transacted;
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
      return transacted;
   }

   public int getAcknowledgeMode() throws JMSException
   {
      return acknowledgeMode;
   }

   public void commit() throws JMSException
   {
      if (!transacted)
         throw new IllegalStateException("Session is not transacted - cannot call commit()");
      sessionDelegate.commit();
   }

   public void rollback() throws JMSException
   {
      if (!transacted)
         throw new IllegalStateException("Session is not transacted - cannot call rollback()");
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
      throw new NotYetImplementedException();
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void run()
   {
      throw new NotYetImplementedException();
   }

   public MessageProducer createProducer(Destination d) throws JMSException
   {
      ProducerDelegate producerDelegate = sessionDelegate.createProducerDelegate(d);
      return new JBossMessageProducer(producerDelegate, d);
   }

  public MessageConsumer createConsumer(Destination d) throws JMSException
  {
     ConsumerDelegate consumerDelegate = sessionDelegate.createConsumerDelegate(d, null);
     return new JBossMessageConsumer(consumerDelegate, d);
  }

  public MessageConsumer createConsumer(Destination d, String messageSelector) throws JMSException
  {
	  ConsumerDelegate consumerDelegate = sessionDelegate.createConsumerDelegate(d, messageSelector);
     return new JBossMessageConsumer(consumerDelegate, d);
  }

   public MessageConsumer createConsumer(Destination destination,
                                         String messageSelector,
                                         boolean NoLocal)
         throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public Queue createQueue(String queueName) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a queue using a TopicSession");
      }
      throw new NotYetImplementedException();
   }

   public Topic createTopic(String topicName) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a topic on a QueueSession");
      }
      throw new NotYetImplementedException();
   }

   public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a durable subscriber on a QueueSession");
      }
      throw new NotYetImplementedException();
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
      throw new NotYetImplementedException();
   }

   public QueueBrowser createBrowser(Queue queue) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_TOPIC_SESSION)
      {
         throw new IllegalStateException("Cannot create a browser on a TopicSession");
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
      JBossTemporaryQueue queue = new JBossTemporaryQueue(connectionDelegate);
      connectionDelegate.addTemporaryDestination(queue);
      return queue;
   }

   public TemporaryTopic createTemporaryTopic() throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot create a temporary topic on a QueueSession");
      }
      JBossTemporaryTopic topic = new JBossTemporaryTopic(connectionDelegate);
      connectionDelegate.addTemporaryDestination(topic);
      return topic;
   }

   public void unsubscribe(String name) throws JMSException
   {
      //As per spec. section 4.11
      if (sessionType == TYPE_QUEUE_SESSION)
      {
         throw new IllegalStateException("Cannot unsubscribe using a QueueSession");
      }
      throw new NotYetImplementedException();
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
      throw new NotYetImplementedException();
   }
   
   // QueueSession implementation
   
   public QueueReceiver createReceiver(Queue queue, String messageSelector)
         throws JMSException
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
