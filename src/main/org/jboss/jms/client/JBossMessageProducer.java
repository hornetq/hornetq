/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.io.Serializable;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class JBossMessageProducer implements MessageProducer, QueueSender, TopicPublisher, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 1080736785725023015L;

   // Static --------------------------------------------------------      

   private static final Logger log = Logger.getLogger(JBossMessageProducer.class);
   
   // Attributes ----------------------------------------------------
   
   protected ProducerDelegate delegate;

   // Constructors --------------------------------------------------
   
   public JBossMessageProducer(ProducerDelegate delegate, Destination destination)
         throws JMSException
   {
      this.delegate = delegate;     
   }
   
   // MessageProducer implementation --------------------------------
   
   public void setDisableMessageID(boolean value) throws JMSException
   {
      log.warn("JBoss Messaging does not support disabling message ID generation");

      // this is to trigger IllegalStateException in case the producer is closed
      //delegate.addMetaData(JMSAdvisor.IS_MESSAGE_ID_DISABLED, Boolean.FALSE);
      
      delegate.setDisableMessageID(value);
   }
   
   public boolean getDisableMessageID() throws JMSException
   {
      //return ((Boolean)delegate.getMetaData(JMSAdvisor.IS_MESSAGE_ID_DISABLED)).booleanValue();
      return delegate.getDisableMessageID();
   }
   
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
     // Boolean b = value ? Boolean.TRUE : Boolean.FALSE;
    //  delegate.addMetaData(JMSAdvisor.IS_MESSAGE_TIMESTAMP_DISABLED, b);
      delegate.setDisableMessageTimestamp(value);
   }
   
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      //return ((Boolean)delegate.
      //      getMetaData(JMSAdvisor.IS_MESSAGE_TIMESTAMP_DISABLED)).booleanValue();
      return delegate.getDisableMessageTimestamp();
   }
   
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      //delegate.addMetaData(JMSAdvisor.DELIVERY_MODE, new Integer(deliveryMode));
      delegate.setDeliveryMode(deliveryMode);
   }
   
   public int getDeliveryMode() throws JMSException
   {
      //return ((Integer)delegate.getMetaData(JMSAdvisor.DELIVERY_MODE)).intValue();
      return delegate.getDeliveryMode();
   }
   
   public void setPriority(int defaultPriority) throws JMSException
   {
      //delegate.addMetaData(JMSAdvisor.PRIORITY, new Integer(defaultPriority));
      delegate.setPriority(defaultPriority);
   }
   
   public int getPriority() throws JMSException
   {
      //return ((Integer)delegate.getMetaData(JMSAdvisor.PRIORITY)).intValue();
      return delegate.getPriority();
   }
   
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      //delegate.addMetaData(JMSAdvisor.TIME_TO_LIVE, new Long(timeToLive));
      delegate.setTimeToLive(timeToLive);
   }
   
   public long getTimeToLive() throws JMSException
   {
      //return ((Long)delegate.getMetaData(JMSAdvisor.TIME_TO_LIVE)).longValue();
      return delegate.getTimeToLive();
   }
   
   public Destination getDestination() throws JMSException
   {
      //return (Destination)delegate.getMetaData(JMSAdvisor.DESTINATION);
      return delegate.getDestination();
   }
   
   public void close() throws JMSException
   {
      delegate.closing();
      delegate.close();
   }
   
   public void send(Message message) throws JMSException
   {
      // by default the message never expires
      send(message, -1, -1, 0);
   }
   
   /**
    * @param timeToLive - 0 means never expire.
    */
   public void send(Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException
   { 
      send(null, message, deliveryMode, priority, timeToLive);
   }
   
   public void send(Destination destination, Message message) throws JMSException
   {      
      send(destination, message, -1, -1, 0);
   }

   public void send(Destination destination,
                    Message m,
                    int deliveryMode,
                    int priority,
                    long timeToLive) throws JMSException
   {
      delegate.send(destination, m, deliveryMode, priority, timeToLive);
   }
   
   // TopicPublisher Implementation
   //--------------------------------------- 
   
   public Topic getTopic() throws JMSException
   {
      return (Topic)getDestination();
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
      return (Queue)getDestination();
   }
   
   // Public --------------------------------------------------------
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
