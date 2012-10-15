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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.hornetq.core.logging.Logger;

/**
 * HornetQQueueSender.
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision:  $
 */
public class HornetQRATopicPublisher extends HornetQRAMessageProducer implements TopicPublisher
{
   /** The logger */
   private static final Logger log = Logger.getLogger(HornetQRATopicPublisher.class);

   /** Whether trace is enabled */
   private static boolean trace = HornetQRATopicPublisher.log.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param producer the producer
    * @param session the session
    */
   public HornetQRATopicPublisher(final TopicPublisher producer, final HornetQRASession session)
   {
      super(producer, session);

      if (HornetQRATopicPublisher.trace)
      {
         HornetQRATopicPublisher.log.trace("constructor(" + producer + ", " + session + ")");
      }
   }

   /**
    * Get the topic
    * @return The topic
    * @exception JMSException Thrown if an error occurs
    */
   public Topic getTopic() throws JMSException
   {
      if (HornetQRATopicPublisher.trace)
      {
         HornetQRATopicPublisher.log.trace("getTopic()");
      }

      return ((TopicPublisher)producer).getTopic();
   }

   /**
    * Publish message
    * @param message The message
    * @param deliveryMode The delivery mode
    * @param priority The priority
    * @param timeToLive The time to live
    * @exception JMSException Thrown if an error occurs
    */
   public void publish(final Message message, final int deliveryMode, final int priority, final long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRATopicPublisher.trace)
         {
            HornetQRATopicPublisher.log.trace("send " + this +
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

         ((TopicPublisher)producer).publish(message, deliveryMode, priority, timeToLive);

         if (HornetQRATopicPublisher.trace)
         {
            HornetQRATopicPublisher.log.trace("sent " + this + " result=" + message);
         }
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Publish message
    * @param message The message
    * @exception JMSException Thrown if an error occurs
    */
   public void publish(final Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRATopicPublisher.trace)
         {
            HornetQRATopicPublisher.log.trace("send " + this + " message=" + message);
         }

         checkState();

         ((TopicPublisher)producer).publish(message);

         if (HornetQRATopicPublisher.trace)
         {
            HornetQRATopicPublisher.log.trace("sent " + this + " result=" + message);
         }
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Publish message
    * @param destination The destination
    * @param message The message
    * @param deliveryMode The delivery mode
    * @param priority The priority
    * @param timeToLive The time to live
    * @exception JMSException Thrown if an error occurs
    */
   public void publish(final Topic destination,
                       final Message message,
                       final int deliveryMode,
                       final int priority,
                       final long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRATopicPublisher.trace)
         {
            HornetQRATopicPublisher.log.trace("send " + this +
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

         ((TopicPublisher)producer).publish(destination, message, deliveryMode, priority, timeToLive);

         if (HornetQRATopicPublisher.trace)
         {
            HornetQRATopicPublisher.log.trace("sent " + this + " result=" + message);
         }
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Publish message
    * @param destination The destination
    * @param message The message
    * @exception JMSException Thrown if an error occurs
    */
   public void publish(final Topic destination, final Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (HornetQRATopicPublisher.trace)
         {
            HornetQRATopicPublisher.log.trace("send " + this + " destination=" + destination + " message=" + message);
         }

         checkState();

         ((TopicPublisher)producer).publish(destination, message);

         if (HornetQRATopicPublisher.trace)
         {
            HornetQRATopicPublisher.log.trace("sent " + this + " result=" + message);
         }
      }
      finally
      {
         session.unlock();
      }
   }
}
