/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.io.Serializable;

import javax.jms.BytesMessage;
import javax.jms.Destination;
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

import org.jboss.jms.destination.JBossTemporaryDestination;

/**
 * A session
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossSession 
   implements Session, QueueSession, TopicSession, 
              XASession, XAQueueSession, XATopicSession
{
   // Constants -----------------------------------------------------

   /** The default message selector */
   private static String defaultSelector = null;

   /** The default no local flag */
   private static boolean defaultNoLocal = false;

   // Attributes ----------------------------------------------------

   /** The delegate */
   private SessionDelegate delegate;

   /** Whether this is an XASession */
   private boolean isXA;

   /** The transacted flag */
   private boolean transacted;

   /** The acknowledgement mode */
   private int acknowledgeMode;

   /** The message listener */
   private MessageListener listener;

	// Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Construct a new JBossSession
    * 
    * @param delegate the session delegate
    * @param isXA whether the session is xa
    * @param transacted whether the session is transacted
    * @param acknowledgeMode the acknowledgement mode
    * @throws JMSException for any error
    */
   public JBossSession(SessionDelegate delegate, boolean isXA, boolean transacted, int acknowledgeMode)
      throws JMSException
   {
      this.delegate = delegate;
      this.isXA = isXA;
      this.transacted = transacted;
      this.acknowledgeMode = acknowledgeMode;
   }

   // Session implementation ----------------------------------------

	public void close() throws JMSException
	{
      delegate.closing();
      delegate.close();
	}

	public void commit() throws JMSException
	{
      if (transacted == false)
         throw new JMSException("Not a transacted session");
      delegate.commit();
	}

	public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException
	{
      if (queue == null)
         throw new JMSException("Null queue");
      return new JBossBrowser(delegate.createBrowser(queue, messageSelector), queue, messageSelector);
	}

	public QueueBrowser createBrowser(Queue queue) throws JMSException
	{
      return createBrowser(queue, defaultSelector);
	}

	public BytesMessage createBytesMessage() throws JMSException
	{
      return delegate.createBytesMessage();
	}

	public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal)
		throws JMSException
	{
		if (destination == null)
         throw new JMSException("Null destination");
      return new JBossConsumer(delegate.createConsumer(destination, null, messageSelector, noLocal), destination, messageSelector, noLocal);
	}

	public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException
	{
      return createConsumer(destination, messageSelector, defaultNoLocal);
	}

	public MessageConsumer createConsumer(Destination destination) throws JMSException
	{
      return createConsumer(destination, defaultSelector, defaultNoLocal);
	}

	public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal)
		throws JMSException
	{
      if (topic == null)
         throw new JMSException("Null topic");
      if (name == null)
         throw new JMSException("Null subscription");
      return (TopicSubscriber) delegate.createConsumer(topic, name, messageSelector, noLocal);
	}

	public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException
	{
      return (TopicSubscriber) createDurableSubscriber(topic, name, null, defaultNoLocal);
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
      return createObjectMessage(null);
	}

	public ObjectMessage createObjectMessage(Serializable object) throws JMSException
	{
      return delegate.createObjectMessage(object);
	}

	public MessageProducer createProducer(Destination destination) throws JMSException
	{
      return new JBossProducer(delegate.createProducer(destination), destination);
	}

	public Queue createQueue(String queueName) throws JMSException
	{
		return (Queue) delegate.getDestination(queueName);
	}

	public StreamMessage createStreamMessage() throws JMSException
	{
      return delegate.createStreamMessage();
	}

	public TemporaryQueue createTemporaryQueue() throws JMSException
	{
      return (TemporaryQueue) delegate.createTempDestination(JBossTemporaryDestination.TEMPORARY_QUEUE);
	}

	public TemporaryTopic createTemporaryTopic() throws JMSException
	{
      return (TemporaryTopic) delegate.createTempDestination(JBossTemporaryDestination.TEMPORARY_TOPIC);
	}

	public TextMessage createTextMessage() throws JMSException
	{
      return createTextMessage(null);
	}

	public TextMessage createTextMessage(String text) throws JMSException
	{
      return delegate.createTextMessage(text);
	}

	public Topic createTopic(String topicName) throws JMSException
	{
      return (Topic) delegate.getDestination(topicName);
	}

	public int getAcknowledgeMode() throws JMSException
	{
      return acknowledgeMode;
	}

	public MessageListener getMessageListener() throws JMSException
	{
      return listener;
	}

	public boolean getTransacted() throws JMSException
	{
      return transacted;
	}

	public void recover() throws JMSException
	{
      delegate.recover();
	}

	public void rollback() throws JMSException
	{
      if (transacted == false)
         throw new JMSException("Not a transacted session");
      delegate.rollback();
	}

	public void run()
	{
      if (listener == null)
         throw new IllegalStateException("No message listener");
      delegate.run();
	}

	public void setMessageListener(MessageListener listener) throws JMSException
	{
      delegate.setMessageListener(listener);
      this.listener = listener;
	}

	public void unsubscribe(String name) throws JMSException
	{
      delegate.unsubscribe(name);
	}

   // XASession implementation --------------------------------------

   public Session getSession() throws JMSException
   {
      return this;
   }

   public XAResource getXAResource()
   {
      if (isXA == false)
         throw new IllegalArgumentException("Not an XASession");
      return delegate.getXAResource();
   }

   // QueueSession implementation -----------------------------------

	public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException
	{
      return (QueueReceiver) createConsumer(queue, messageSelector);
	}

	public QueueReceiver createReceiver(Queue queue) throws JMSException
	{
      return (QueueReceiver) createConsumer(queue);
	}

	public QueueSender createSender(Queue queue) throws JMSException
	{
      return (QueueSender) createProducer(queue);
	}

   // TopicSession implementation -----------------------------------

   public TopicSubscriber createSubscriber(Topic topic) throws JMSException
   {
      return (TopicSubscriber) createConsumer(topic);
   }

   public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException
   {
      return (TopicSubscriber) createConsumer(topic, messageSelector, noLocal);
   }

   public TopicPublisher createPublisher(Topic topic) throws JMSException
   {
      return (TopicPublisher) createProducer(topic);
   }

   // XAQueueSession implementation ---------------------------------

	public QueueSession getQueueSession() throws JMSException
	{
      return (QueueSession) getSession();
	}

   // XATopicSession implementation ---------------------------------

	public TopicSession getTopicSession() throws JMSException
	{
      return (TopicSession) getSession();
	}

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
