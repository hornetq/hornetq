/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.facade;

import org.jboss.messaging.jms.client.ConsumerDelegate;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * A consumer
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossConsumer 
   implements MessageConsumer, QueueReceiver, TopicSubscriber
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The delegate */
   private ConsumerDelegate delegate;

   /** The default destination */
   private Destination defaultDestination;

   /** The message listener */
   private MessageListener listener;

   /** The message selector */
   private String selector;

   /** The no local flag */
   private boolean noLocal;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Create a new JBossBrowser
    * 
    * @param delegate the delegate
    * @param destination the destination
    * @param selector the selector
    * @param noLocal the no local flag
    * @throws JMSException for any error
    */
   public JBossConsumer(ConsumerDelegate delegate, Destination destination, String selector, boolean noLocal)
      throws JMSException
   {
      this.delegate = delegate;
      this.defaultDestination = destination;
      this.selector = selector;
      this.noLocal = noLocal;
   }

   // Public --------------------------------------------------------

   public Destination getDestination() throws JMSException
   {
      return defaultDestination;
   }

   // MessageConsumer implementation --------------------------------

	public void close() throws JMSException
	{
      delegate.closing();
      delegate.close();
	}

	public MessageListener getMessageListener() throws JMSException
	{
      return listener;
	}

	public String getMessageSelector() throws JMSException
	{
      return selector;
	}

	public Message receive() throws JMSException
	{
      return receive(0);
	}

	public Message receive(long timeout) throws JMSException
	{
      return delegate.receive(timeout);
	}

	public Message receiveNoWait() throws JMSException
	{
      return receive(-1);
	}

	public void setMessageListener(MessageListener listener) throws JMSException
	{
      delegate.setMessageListener(listener);
      this.listener = listener;
	}

   // QueueReceiver implementation ----------------------------------

	public Queue getQueue() throws JMSException
	{
      return (Queue) getDestination();
	}

   // TopicSubscriber implementation --------------------------------

	public boolean getNoLocal() throws JMSException
	{
      return noLocal;
	}

	public Topic getTopic() throws JMSException
	{
      return (Topic) getDestination();
	}

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
