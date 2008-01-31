/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.jms.client;

import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossMessageProducer implements MessageProducer, QueueSender, TopicPublisher
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------      

   private static final Logger log = Logger.getLogger(JBossMessageProducer.class);
   
   // Attributes ----------------------------------------------------
   
   protected org.jboss.jms.client.api.ClientProducer producer;

   // Constructors --------------------------------------------------
   
   public JBossMessageProducer(org.jboss.jms.client.api.ClientProducer producer)
   {
      this.producer = producer;     
   }
   
   // MessageProducer implementation --------------------------------
   
   public void setDisableMessageID(boolean value) throws JMSException
   {
      log.warn("JBoss Messaging does not support disabling message ID generation");

      producer.setDisableMessageID(value);
   }
   
   public boolean getDisableMessageID() throws JMSException
   {
      return producer.isDisableMessageID();
   }
   
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      producer.setDisableMessageTimestamp(value);
   }
   
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      return producer.isDisableMessageTimestamp();
   }
   
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      producer.setDeliveryMode(deliveryMode);
   }
   
   public int getDeliveryMode() throws JMSException
   {
      return producer.getDeliveryMode();
   }
   
   public void setPriority(int defaultPriority) throws JMSException
   {
      producer.setPriority(defaultPriority);
   }
   
   public int getPriority() throws JMSException
   {
      return producer.getPriority();
   }
   
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      producer.setTimeToLive(timeToLive);
   }
   
   public long getTimeToLive() throws JMSException
   {
      return producer.getTimeToLive();
   }
   
   public Destination getDestination() throws JMSException
   {
      return producer.getDestination();
   }
   
   public void close() throws JMSException
   {
      producer.closing();
      producer.close();
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
      if (destination != null && !(destination instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Not a JBossDestination:" + destination);
      }

      producer.send((JBossDestination)destination, m, deliveryMode, priority, timeToLive);
   }


   // TopicPublisher Implementation ---------------------------------

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

   // QueueSender Implementation ------------------------------------

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

   public org.jboss.jms.client.api.ClientProducer getDelegate()
   {
      return producer;
   }

   public String toString()
   {
      return "JBossMessageProducer->" + producer;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
