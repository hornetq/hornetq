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

import java.io.Serializable;

import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossMessageProducer implements MessageProducer, QueueSender, TopicPublisher, Serializable
{
   // Constants -----------------------------------------------------
   
   private static final long serialVersionUID = 1080736785725023015L;

   // Static --------------------------------------------------------      

   private static final Logger log = Logger.getLogger(JBossMessageProducer.class);
   
   // Attributes ----------------------------------------------------
   
   protected ProducerDelegate delegate;

   // Constructors --------------------------------------------------
   
   public JBossMessageProducer(ProducerDelegate delegate)
   {
      this.delegate = delegate;     
   }
   
   // MessageProducer implementation --------------------------------
   
   public void setDisableMessageID(boolean value) throws JMSException
   {
      log.warn("JBoss Messaging does not support disabling message ID generation");

      delegate.setDisableMessageID(value);
   }
   
   public boolean getDisableMessageID() throws JMSException
   {
      return delegate.getDisableMessageID();
   }
   
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      delegate.setDisableMessageTimestamp(value);
   }
   
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      return delegate.getDisableMessageTimestamp();
   }
   
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      delegate.setDeliveryMode(deliveryMode);
   }
   
   public int getDeliveryMode() throws JMSException
   {
      return delegate.getDeliveryMode();
   }
   
   public void setPriority(int defaultPriority) throws JMSException
   {
      delegate.setPriority(defaultPriority);
   }
   
   public int getPriority() throws JMSException
   {
      return delegate.getPriority();
   }
   
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      delegate.setTimeToLive(timeToLive);
   }
   
   public long getTimeToLive() throws JMSException
   {
      return delegate.getTimeToLive();
   }
   
   public Destination getDestination() throws JMSException
   {
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

      delegate.send((JBossDestination)destination, m, deliveryMode, priority, timeToLive);
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

   public ProducerDelegate getDelegate()
   {
      return delegate;
   }

   public String toString()
   {
      return "JBossMessageProducer->" + delegate;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
