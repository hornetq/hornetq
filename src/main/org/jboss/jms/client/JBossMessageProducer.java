/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.jms.delegate.ProducerDelegate;

import javax.jms.MessageProducer;
import javax.jms.JMSException;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.DeliveryMode;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossMessageProducer implements MessageProducer
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ProducerDelegate delegate;

   protected int deliveryMode = DeliveryMode.PERSISTENT;

   // Constructors --------------------------------------------------

   public JBossMessageProducer(ProducerDelegate delegate)
   {
      this.delegate = delegate;
   }

   // MessageProducer implementation --------------------------------

   public void setDisableMessageID(boolean value) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public boolean getDisableMessageID() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public boolean getDisableMessageTimestamp() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      this.deliveryMode = deliveryMode;
   }

   public int getDeliveryMode() throws JMSException
   {
      return deliveryMode;
   }

   public void setPriority(int defaultPriority) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public int getPriority() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void setTimeToLive(long timeToLive) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public long getTimeToLive() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public Destination getDestination() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void close() throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void send(Message message) throws JMSException
   {
      configure(message);
      delegate.send(message);
   }

   public void send(Message message, int deliveryMode, int priority, long timeToLive)
         throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void send(Destination destination, Message message) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   public void send(Destination destination,
                    Message message,
                    int deliveryMode,
                    int priority,
                    long timeToLive) throws JMSException
   {
      throw new NotYetImplementedException();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * Set the headers.
    */
   protected void configure(Message m) throws JMSException
   {
      m.setJMSDeliveryMode(deliveryMode);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
