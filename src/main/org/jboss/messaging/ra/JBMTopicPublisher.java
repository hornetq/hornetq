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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.jboss.messaging.core.logging.Logger;

/**
 * JBMQueueSender.
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision:  $
 */
public class JBMTopicPublisher extends JBMMessageProducer implements TopicPublisher
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMTopicPublisher.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /**
    * Create a new wrapper
    * @param producer the producer
    * @param session the session
    */
   public JBMTopicPublisher(TopicPublisher producer, JBMSession session)
   {
      super(producer, session);

      if (trace)
         log.trace("constructor(" + producer + ", " + session + ")");
   }

   /**
    * Get the topic
    * @return The topic
    * @exception JMSException Thrown if an error occurs
    */
   public Topic getTopic() throws JMSException
   {
      if (trace)
         log.trace("getTopic()");

      return ((TopicPublisher) producer).getTopic();
   }

   /**
    * Publish message
    * @param message The message
    * @param deliveryMode The delivery mode
    * @param priority The priority
    * @param timeToLive The time to live
    * @exception JMSException Thrown if an error occurs
    */
   public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("send " + this  + " message=" + message + " deliveryMode=" + deliveryMode + " priority=" + priority + " ttl=" + timeToLive);

         checkState();

         ((TopicPublisher) producer).publish(message, deliveryMode, priority, timeToLive);

         if (trace)
            log.trace("sent " + this + " result=" + message);
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
   public void publish(Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("send " + this + " message=" + message);

         checkState();

         ((TopicPublisher) producer).publish(message);

         if (trace)
            log.trace("sent " + this + " result=" + message);
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
   public void publish(Topic destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("send " + this + " destination=" + destination + " message=" + message + " deliveryMode=" + deliveryMode + " priority=" + priority + " ttl=" + timeToLive);

         checkState();

         ((TopicPublisher) producer).publish(destination, message, deliveryMode, priority, timeToLive);

         if (trace)
            log.trace("sent " + this + " result=" + message);
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
   public void publish(Topic destination, Message message) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("send " + this + " destination=" + destination + " message=" + message);

         checkState();

         ((TopicPublisher) producer).publish(destination, message);

         if (trace)
            log.trace("sent " + this + " result=" + message);
      }
      finally
      {
         session.unlock();
      }
   }
}
