/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.messaging.util.NotYetImplementedException;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.MessageConsumer;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossMessageConsumer implements MessageConsumer
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ConsumerDelegate delegate;

   // Constructors --------------------------------------------------

   public JBossMessageConsumer(ConsumerDelegate delegate)
   {
      this.delegate = delegate;
   }

   // MessageConsumer implementation --------------------------------

   public String getMessageSelector() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public MessageListener getMessageListener() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
      throw new NotYetImplementedException();
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
      throw new NotYetImplementedException();
   }

   public void close() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
