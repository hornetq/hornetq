/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.jboss.jms.delegate.ConsumerDelegate;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class JBossMessageConsumer implements MessageConsumer, QueueReceiver, TopicSubscriber, Serializable
{   
   // Constants -----------------------------------------------------  
   
   private static final long serialVersionUID = -8776908463975467851L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConsumerDelegate delegate;

   // Constructors --------------------------------------------------

   public JBossMessageConsumer(ConsumerDelegate delegate) throws JMSException
   {      
      this.delegate = delegate;
   }

   // MessageConsumer implementation --------------------------------

   public String getMessageSelector() throws JMSException
   {
      //return (String)delegate.getMetaData(JMSAdvisor.SELECTOR);
      return delegate.getMessageSelector();
   }


   public MessageListener getMessageListener() throws JMSException
   {
      return delegate.getMessageListener();
   }


   public void setMessageListener(MessageListener listener) throws JMSException
   {
      delegate.setMessageListener(listener);
   }


   public Message receive() throws JMSException
   {
      return delegate.receive(0);
   }


   public Message receive(long timeout) throws JMSException
   {
      return delegate.receive(timeout);
   }


   public Message receiveNoWait() throws JMSException
   {
      return delegate.receive(-1);
   }


   public void close() throws JMSException
   {
      delegate.close();
   }


   // QueueReceiver implementation ----------------------------------


   public Queue getQueue() throws JMSException
   {
      return (Queue)delegate.getDestination();
   }

   // TopicSubscriber implementation --------------------------------


   public Topic getTopic() throws JMSException
   {
      return (Topic)delegate.getDestination();
   }


   public boolean getNoLocal() throws JMSException
   {
      return delegate.getNoLocal();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
