/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

import org.jboss.messaging.core.logging.Logger;

/**
 * JBMMessageProducer.
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision:  $
 */
public class JBMMessageProducer implements MessageProducer
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMMessageProducer.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The wrapped message producer */
   protected MessageProducer producer;
   
   /** The session for this consumer */
   protected JBMSession session;

   /**
    * Create a new wrapper
    * @param producer the producer
    * @param session the session
    */
   public JBMMessageProducer(MessageProducer producer, JBMSession session)
   {
      this.producer = producer;
      this.session = session;
      
      if (trace)
         log.trace("new JBMMessageProducer " + this + " producer=" + producer + " session=" + session);
   }

   /**
    * Close
    * @exception JMSException Thrown if an error occurs
    */
   public void close() throws JMSException
   {
      if (trace)
         log.trace("close " + this);
      try
      {
         closeProducer();
      }
      finally
      {
         session.removeProducer(this);
      }
   }

   /**
    * Send message
    * @param destination The destination
    * @param message The message
    * @param deliveryMode The delivery mode
    * @param priority The priority
    * @param timeToLive The time to live
    * @exception JMSException Thrown if an error occurs
    */
   public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("send " + this + " destination=" + destination + " message=" + message + " deliveryMode=" + deliveryMode + " priority=" + priority + " ttl=" + timeToLive);

         checkState();

         producer.send(destination, message, deliveryMode, priority, timeToLive);

         if (trace)
            log.trace("sent " + this + " result=" + message);
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Send message
    * @param destination The destination
    * @param message The message
    * @exception JMSException Thrown if an error occurs
    */
   public void send(Destination destination, Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("send " + this + " destination=" + destination + " message=" + message);

         checkState();

         producer.send(destination, message);

         if (trace)
            log.trace("sent " + this + " result=" + message);
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Send message
    * @param message The message
    * @param deliveryMode The delivery mode
    * @param priority The priority
    * @param timeToLive The time to live
    * @exception JMSException Thrown if an error occurs
    */
   public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("send " + this + " message=" + message + " deliveryMode=" + deliveryMode + " priority=" + priority + " ttl=" + timeToLive);

         checkState();

         producer.send(message, deliveryMode, priority, timeToLive);

         if (trace)
            log.trace("sent " + this + " result=" + message);
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Send message
    * @param message The message
    * @exception JMSException Thrown if an error occurs
    */
   public void send(Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("send " + this + " message=" + message);

         checkState();

         producer.send(message);

         if (trace)
            log.trace("sent " + this + " result=" + message);
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Get the delivery mode
    * @return The mode
    * @exception JMSException Thrown if an error occurs
    */
   public int getDeliveryMode() throws JMSException
   {
      if (trace)
         log.trace("getDeliveryMode()");

      return producer.getDeliveryMode();
   }

   /**
    * Get the destination
    * @return The destination
    * @exception JMSException Thrown if an error occurs
    */
   public Destination getDestination() throws JMSException
   {
      if (trace)
         log.trace("getDestination()");

      return producer.getDestination();
   }

   /**
    * Disable message id
    * @return True if disable
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getDisableMessageID() throws JMSException
   {
      if (trace)
         log.trace("getDisableMessageID()");

      return producer.getDisableMessageID();
   }

   /**
    * Disable message timestamp
    * @return True if disable
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      if (trace)
         log.trace("getDisableMessageTimestamp()");

      return producer.getDisableMessageTimestamp();
   }

   /**
    * Get the priority
    * @return The priority
    * @exception JMSException Thrown if an error occurs
    */
   public int getPriority() throws JMSException
   {
      if (trace)
         log.trace("getPriority()");

      return producer.getPriority();
   }

   /**
    * Get the time to live
    * @return The ttl
    * @exception JMSException Thrown if an error occurs
    */
   public long getTimeToLive() throws JMSException
   {
      if (trace)
         log.trace("getTimeToLive()");

      return producer.getTimeToLive();
   }

   /**
    * Set the delivery mode
    * @param deliveryMode The mode
    * @exception JMSException Thrown if an error occurs
    */
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      if (trace)
         log.trace("setDeliveryMode(" + deliveryMode + ")");

      producer.setDeliveryMode(deliveryMode);
   }

   /**
    * Set disable message id
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setDisableMessageID(boolean value) throws JMSException
   {
      if (trace)
         log.trace("setDisableMessageID(" + value + ")");

      producer.setDisableMessageID(value);
   }

   /**
    * Set disable message timestamp
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      if (trace)
         log.trace("setDisableMessageTimestamp(" + value + ")");

      producer.setDisableMessageTimestamp(value);
   }

   /**
    * Set the priority
    * @param defaultPriority The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setPriority(int defaultPriority) throws JMSException
   {
      if (trace)
         log.trace("setPriority(" + defaultPriority + ")");

      producer.setPriority(defaultPriority);
   }

   /**
    * Set the ttl
    * @param timeToLive The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      if (trace)
         log.trace("setTimeToLive(" + timeToLive + ")");

      producer.setTimeToLive(timeToLive);
   }

   /**
    * Check state
    * @exception JMSException Thrown if an error occurs
    */
   void checkState() throws JMSException
   {
   }

   /**
    * Close producer
    * @exception JMSException Thrown if an error occurs
    */
   void closeProducer() throws JMSException
   {
      producer.close();
   }
}
