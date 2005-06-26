/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.container.JMSAdvisor;
import org.jboss.logging.Logger;
import org.jboss.util.id.GUID;

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
 *
 * $Id$
 */
class JBossMessageProducer implements MessageProducer, QueueSender, TopicPublisher
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JBossMessageProducer.class);
   
   // Attributes ----------------------------------------------------
   
   protected ProducerDelegate delegate;

   // Constructors --------------------------------------------------
   
   public JBossMessageProducer(ProducerDelegate delegate, Destination destination)
         throws JMSException
   {
      this.delegate = delegate;
      setDeliveryMode(DeliveryMode.PERSISTENT);
      setDisableMessageTimestamp(false);
      setPriority(4);
      setTimeToLive(0l);
      delegate.addMetaData(JMSAdvisor.IS_MESSAGE_ID_DISABLED, Boolean.FALSE);
      delegate.addMetaData(JMSAdvisor.DESTINATION, destination);

   }
   
   // MessageProducer implementation --------------------------------
   
   public void setDisableMessageID(boolean value) throws JMSException
   {
      log.warn("JBoss Messaging does not support disabling message ID generation");

      // this is to trigger IllegalStateException in case the producer is closed
      delegate.addMetaData(JMSAdvisor.IS_MESSAGE_ID_DISABLED, Boolean.FALSE);
   }
   
   public boolean getDisableMessageID() throws JMSException
   {
      return ((Boolean)delegate.getMetaData(JMSAdvisor.IS_MESSAGE_ID_DISABLED)).booleanValue();
   }
   
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      Boolean b = value ? Boolean.TRUE : Boolean.FALSE;
      delegate.addMetaData(JMSAdvisor.IS_MESSAGE_TIMESTAMP_DISABLED, b);
   }
   
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      return ((Boolean)delegate.
            getMetaData(JMSAdvisor.IS_MESSAGE_TIMESTAMP_DISABLED)).booleanValue();
   }
   
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      delegate.addMetaData(JMSAdvisor.DELIVERY_MODE, new Integer(deliveryMode));
   }
   
   public int getDeliveryMode() throws JMSException
   {
      return ((Integer)delegate.getMetaData(JMSAdvisor.DELIVERY_MODE)).intValue();
   }
   
   public void setPriority(int defaultPriority) throws JMSException
   {
      delegate.addMetaData(JMSAdvisor.PRIORITY, new Integer(defaultPriority));
   }
   
   public int getPriority() throws JMSException
   {
      return ((Integer)delegate.getMetaData(JMSAdvisor.PRIORITY)).intValue();
   }
   
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      delegate.addMetaData(JMSAdvisor.TIME_TO_LIVE, new Long(timeToLive));
   }
   
   public long getTimeToLive() throws JMSException
   {
      return ((Long)delegate.getMetaData(JMSAdvisor.TIME_TO_LIVE)).longValue();
   }
   
   public Destination getDestination() throws JMSException
   {
      return (Destination)delegate.getMetaData(JMSAdvisor.DESTINATION);
   }
   
   public void close() throws JMSException
   {
      delegate.closing();
      delegate.close();
   }
   
   public void send(Message message) throws JMSException
   {
      // by default the message never expires
      send(message, -1, -1, Long.MIN_VALUE);
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
      send(destination, message, -1, -1, Long.MIN_VALUE);
   }

   public void send(Destination destination,
                    Message m,
                    int deliveryMode,
                    int priority,
                    long timeToLive) throws JMSException
   {
      if (m instanceof JBossBytesMessage)
      {
         if (log.isTraceEnabled()) { log.trace("Calling reset()"); }
         ((JBossBytesMessage)m).reset();
      }

            
      //Section 3.9 of JMS1.1 spec states:
      //"After sending a message, a client may retain and modify it without affecting
      //the message that has been sent. The same message object may be sent multiple
      //times."
      //So we clone the message
      JBossMessage cloned = ((JBossMessage)m).doClone();
      String messageID = generateMessageID();
      cloned.setJMSMessageID(messageID);
      m.setJMSMessageID(messageID);
      cloned.setPropertiesReadWrite(false);
      
      

      delegate.send(destination, cloned, deliveryMode, priority, timeToLive);

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
   
   protected String generateMessageID()
   {
      StringBuffer sb = new StringBuffer("ID:");
      sb.append(new GUID().toString());
      return sb.toString();
   }
   
   // Inner classes -------------------------------------------------
}
