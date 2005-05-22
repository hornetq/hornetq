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
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.QueueSender;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class JBossMessageProducer
   implements MessageProducer, QueueSender, TopicPublisher
{
   // Constants -----------------------------------------------------  

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossMessageProducer.class);
   
   // Attributes ----------------------------------------------------

   protected ProducerDelegate delegate;

   protected int deliveryMode;
   protected boolean isMessageIDDisabled;
   protected boolean isTimestampDisabled;
   protected int priority;
   
   protected Destination destination;
   
   // Constructors --------------------------------------------------

   public JBossMessageProducer(ProducerDelegate delegate, Destination destination)
   {      
      this.delegate = delegate;
      this.destination = destination;
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
      return destination;
   }

   public void close() throws JMSException
   {
      //Don't need to do anything
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
      configure(message, deliveryMode, priority, timeToLive, this.destination);
      delegate.send(message);     
   }

   public void send(Destination destination, Message message) throws JMSException
   {
      configure(message, this.deliveryMode, this.priority, 0, destination);
      delegate.send(message);    
   }

   public void send(Destination destination,
                    Message message,
                    int deliveryMode,
                    int priority,
                    long timeToLive) throws JMSException
   {
      configure(message, deliveryMode, priority, timeToLive, destination);
      delegate.send(message);
   }
   
   // TopicPublisher Implementation
   //--------------------------------------- 
   
   public Topic getTopic() throws JMSException
   {
      return (Topic)destination;
   }
   
   public void publish(Message message) throws JMSException
   {
      send(message);
   }
   
   public void publish(Topic topic, Message message) throws JMSException
   {
      send(topic, message);
   }
   
   public void publish(Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException
   {
      send(message, deliveryMode, priority, timeToLive);
   }
   
   public void publish(Topic topic, Message message, int deliveryMode, 
                       int priority, long timeToLive) throws JMSException
   {
      send(topic, message, deliveryMode, priority, timeToLive);
   }
   
   // QueueSender Implementation
   //---------------------------------------
   public void send(Queue queue, Message message) throws JMSException
   {
      send((Destination)queue, message);
   }
   
   public void send(Queue queue, Message message, int deliveryMode, int priority,
                    long timeToLive) throws JMSException
   {
      send((Destination)queue, message, deliveryMode, priority, timeToLive);
   }
   
   public Queue getQueue() throws JMSException
   {
      return (Queue)destination;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * Set the headers.
    */
   protected void configure(Message m, int deliveryMode, int priority,
                            long timeToLive, Destination dest)
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
      
      m.setJMSDestination(dest);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
