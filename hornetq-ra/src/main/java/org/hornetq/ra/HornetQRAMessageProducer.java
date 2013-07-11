/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.ra;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;


/**
 * HornetQMessageProducer.
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 */
public class HornetQRAMessageProducer implements MessageProducer
{
   /** Whether trace is enabled */
   private static boolean trace = HornetQRALogger.LOGGER.isTraceEnabled();

   /** The wrapped message producer */
   protected MessageProducer producer;

   /** The session for this consumer */
   protected HornetQRASession session;

   /**
    * Create a new wrapper
    * @param producer the producer
    * @param session the session
    */
   public HornetQRAMessageProducer(final MessageProducer producer, final HornetQRASession session)
   {
      this.producer = producer;
      this.session = session;

      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("new HornetQMessageProducer " + this +
                                            " producer=" +
                                            producer +
                                            " session=" +
                                            session);
      }
   }

   /**
    * Close
    * @exception JMSException Thrown if an error occurs
    */
   public void close() throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("close " + this);
      }
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
   public void send(final Destination destination,
                    final Message message,
                    final int deliveryMode,
                    final int priority,
                    final long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAMessageProducer.trace)
         {
            HornetQRALogger.LOGGER.trace("send " + this +
                                               " destination=" +
                                               destination +
                                               " message=" +
                                               message +
                                               " deliveryMode=" +
                                               deliveryMode +
                                               " priority=" +
                                               priority +
                                               " ttl=" +
                                               timeToLive);
         }

         checkState();

         producer.send(destination, message, deliveryMode, priority, timeToLive);

         if (HornetQRAMessageProducer.trace)
         {
            HornetQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
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
   public void send(final Destination destination, final Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAMessageProducer.trace)
         {
            HornetQRALogger.LOGGER.trace("send " + this + " destination=" + destination + " message=" + message);
         }

         checkState();

         producer.send(destination, message);

         if (HornetQRAMessageProducer.trace)
         {
            HornetQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
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
   public void send(final Message message, final int deliveryMode, final int priority, final long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAMessageProducer.trace)
         {
            HornetQRALogger.LOGGER.trace("send " + this +
                                               " message=" +
                                               message +
                                               " deliveryMode=" +
                                               deliveryMode +
                                               " priority=" +
                                               priority +
                                               " ttl=" +
                                               timeToLive);
         }

         checkState();

         producer.send(message, deliveryMode, priority, timeToLive);

         if (HornetQRAMessageProducer.trace)
         {
            HornetQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
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
   public void send(final Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRAMessageProducer.trace)
         {
            HornetQRALogger.LOGGER.trace("send " + this + " message=" + message);
         }

         checkState();

         producer.send(message);

         if (HornetQRAMessageProducer.trace)
         {
            HornetQRALogger.LOGGER.trace("sent " + this + " result=" + message);
         }
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
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("getDeliveryMode()");
      }

      return producer.getDeliveryMode();
   }

   /**
    * Get the destination
    * @return The destination
    * @exception JMSException Thrown if an error occurs
    */
   public Destination getDestination() throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("getDestination()");
      }

      return producer.getDestination();
   }

   /**
    * Disable message id
    * @return True if disable
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getDisableMessageID() throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("getDisableMessageID()");
      }

      return producer.getDisableMessageID();
   }

   /**
    * Disable message timestamp
    * @return True if disable
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("getDisableMessageTimestamp()");
      }

      return producer.getDisableMessageTimestamp();
   }

   /**
    * Get the priority
    * @return The priority
    * @exception JMSException Thrown if an error occurs
    */
   public int getPriority() throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("getPriority()");
      }

      return producer.getPriority();
   }

   /**
    * Get the time to live
    * @return The ttl
    * @exception JMSException Thrown if an error occurs
    */
   public long getTimeToLive() throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("getTimeToLive()");
      }

      return producer.getTimeToLive();
   }

   /**
    * Set the delivery mode
    * @param deliveryMode The mode
    * @exception JMSException Thrown if an error occurs
    */
   public void setDeliveryMode(final int deliveryMode) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("setDeliveryMode(" + deliveryMode + ")");
      }

      producer.setDeliveryMode(deliveryMode);
   }

   /**
    * Set disable message id
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setDisableMessageID(final boolean value) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("setDisableMessageID(" + value + ")");
      }

      producer.setDisableMessageID(value);
   }

   /**
    * Set disable message timestamp
    * @param value The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setDisableMessageTimestamp(final boolean value) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("setDisableMessageTimestamp(" + value + ")");
      }

      producer.setDisableMessageTimestamp(value);
   }

   /**
    * Set the priority
    * @param defaultPriority The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setPriority(final int defaultPriority) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("setPriority(" + defaultPriority + ")");
      }

      producer.setPriority(defaultPriority);
   }

   /**
    * Set the ttl
    * @param timeToLive The value
    * @exception JMSException Thrown if an error occurs
    */
   public void setTimeToLive(final long timeToLive) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("setTimeToLive(" + timeToLive + ")");
      }

      producer.setTimeToLive(timeToLive);
   }

   @Override
   public void setDeliveryDelay(long deliveryDelay) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("setDeliveryDelay(" + deliveryDelay + ")");
      }
      producer.setDeliveryDelay(deliveryDelay);
   }

   @Override
   public long getDeliveryDelay() throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("getDeliveryDelay()");
      }
      return producer.getDeliveryDelay();
   }

   @Override
   public void send(Message message, CompletionListener completionListener) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("send(" + message + ", " + completionListener + ")");
      }
      producer.send(message, completionListener);
   }

   @Override
   public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("send(" + message + ", " + deliveryMode + ", " + priority + ", " + timeToLive +
                  ", " + completionListener + ")");
      }
      producer.send(message, deliveryMode, priority, timeToLive, completionListener);
   }

   @Override
   public void send(Destination destination, Message message, CompletionListener completionListener) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("send(" + destination + ", " + message + ", " + completionListener + ")");
      }
      producer.send(destination, message, completionListener);
   }

   @Override
   public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException
   {
      if (HornetQRAMessageProducer.trace)
      {
         HornetQRALogger.LOGGER.trace("send(" + destination + ", " + message + ", " + deliveryMode + ", " + priority +
                  ", " + timeToLive + ", " + completionListener + ")");
      }
      producer.send(destination, message, deliveryMode, priority, timeToLive, completionListener);
   }

    /**
    * Check state
    * @exception JMSException Thrown if an error occurs
    */
   void checkState() throws JMSException
   {
      session.checkState();
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
