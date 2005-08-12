/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.io.Serializable;

import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.server.container.JMSAdvisor;

import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.MessageConsumer;
import javax.jms.QueueReceiver;
import javax.jms.TopicSubscriber;
import javax.jms.MessageListener;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
class JBossMessageConsumer implements MessageConsumer, QueueReceiver, TopicSubscriber, Serializable
{
   // Constants -----------------------------------------------------  

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConsumerDelegate delegate;

   // Constructors --------------------------------------------------

   public JBossMessageConsumer(ConsumerDelegate delegate, boolean noLocal) throws JMSException
   {      
      this.delegate = delegate;

      delegate.addMetaData(JMSAdvisor.NO_LOCAL, noLocal ? Boolean.TRUE : Boolean.FALSE);

       // make sure DESTINATION is TRANSIENT, to avoid unnecessary network traffic
      Destination d = (Destination)delegate.removeMetaData(JMSAdvisor.DESTINATION);
      delegate.addMetaData(JMSAdvisor.DESTINATION, d); // add as TRANSIENT
   }

   // MessageConsumer implementation --------------------------------

   public String getMessageSelector() throws JMSException
   {
      return (String)delegate.getMetaData(JMSAdvisor.SELECTOR);
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
      delegate.closing();
      delegate.close();
   }


   // QueueReceiver implementation ----------------------------------


   public Queue getQueue() throws JMSException
   {
      return (Queue)delegate.getMetaData(JMSAdvisor.DESTINATION);
   }

   // TopicSubscriber implementation --------------------------------


   public Topic getTopic() throws JMSException
   {
      return (Topic)delegate.getMetaData(JMSAdvisor.DESTINATION);
   }


   public boolean getNoLocal() throws JMSException
   {
      return ((Boolean)delegate.getMetaData(JMSAdvisor.NO_LOCAL)).booleanValue();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
