/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.logging.Logger;

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

   private static final Logger log = Logger.getLogger(JBossMessageProducer.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ProducerDelegate delegate;

   protected int deliveryMode;
   protected boolean isMessageIDDisabled;
   protected boolean isTimestampDisabled;
   protected int priority;

   // Constructors --------------------------------------------------

   public JBossMessageProducer(ProducerDelegate delegate)
   {
      this.delegate = delegate;
      deliveryMode = DeliveryMode.PERSISTENT;
      isMessageIDDisabled = false;
      isTimestampDisabled = false;
      priority = 4;
   }

   // MessageProducer implementation --------------------------------

   public void setDisableMessageID(boolean value) throws JMSException
   {
      log.warn("JBoss Messaging does not support disabling message ID generation");
   }

   public boolean getDisableMessageID() throws JMSException
   {
      return isMessageIDDisabled;
   }

   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      isTimestampDisabled = value;
   }

   public boolean getDisableMessageTimestamp() throws JMSException
   {
      return isTimestampDisabled;
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
      // by default the message never expires
      send(message, this.deliveryMode, this.priority, 0l);
   }

   /**
    * @param timeToLive - 0 means never expire.
    */
   public void send(Message message, int deliveryMode, int priority, long timeToLive)
         throws JMSException
   {
      configure(message, deliveryMode, priority, timeToLive);
      delegate.send(message);
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
   protected void configure(Message m, int deliveryMode, int priority, long timeToLive)
         throws JMSException
   {
      m.setJMSDeliveryMode(deliveryMode);
      if (isTimestampDisabled)
      {
         m.setJMSTimestamp(0l);
      }
      else
      {
         m.setJMSTimestamp(System.currentTimeMillis());
      }
      // TODO priority

      if (timeToLive == 0)
      {
         m.setJMSExpiration(Long.MAX_VALUE);
      }
      else
      {
         m.setJMSExpiration(System.currentTimeMillis() + timeToLive);
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
