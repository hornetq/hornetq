/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.jms.delegate.ConsumerDelegate;

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
class JBossMessageConsumer implements MessageConsumer, QueueReceiver, TopicSubscriber
{
   // Constants -----------------------------------------------------  

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConsumerDelegate delegate;
   protected Destination destination;
   protected boolean noLocal;
   
   // Constructors --------------------------------------------------

   public JBossMessageConsumer(ConsumerDelegate delegate,
                              Destination destination,
                              boolean noLocal)
   {      
      this.delegate = delegate;
      this.destination = destination;
      this.noLocal = noLocal;
   }

   // MessageConsumer implementation --------------------------------

   public String getMessageSelector() throws JMSException
   {
      throw new NotYetImplementedException();
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
      return (Queue)destination;
   }

   // TopicSubscriber implementation --------------------------------


   public Topic getTopic() throws JMSException
   {
      return (Topic)destination;
   }


   public boolean getNoLocal() throws JMSException
   {
      return noLocal;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
