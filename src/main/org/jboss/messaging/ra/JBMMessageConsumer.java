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

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.jboss.messaging.core.logging.Logger;

/**
 * A wrapper for a message consumer
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMMessageConsumer implements MessageConsumer
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMMessageConsumer.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The wrapped message consumer */
   protected MessageConsumer consumer;
   
   /** The session for this consumer */
   protected JBMSession session;

   /**
    * Create a new wrapper
    * @param consumer the consumer
    * @param session the session
    */
   public JBMMessageConsumer(MessageConsumer consumer, JBMSession session)
   {
      this.consumer = consumer;
      this.session = session;
      
      if (trace)
         log.trace("new JBMMessageConsumer " + this + " consumer=" + consumer + " session=" + session);
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
         closeConsumer();
      }
      finally
      {
         session.removeConsumer(this);
      }
   }

   /**
    * Check state
    * @exception JMSException Thrown if an error occurs
    */
   void checkState() throws JMSException
   {
      if (trace)
         log.trace("checkState()");
   }
   
   /**
    * Get message listener
    * @return The listener
    * @exception JMSException Thrown if an error occurs
    */
   public MessageListener getMessageListener() throws JMSException
   {
      if (trace)
         log.trace("getMessageListener()");

      checkState();
      session.checkStrict();
      return consumer.getMessageListener();
   }
   
   /**
    * Set message listener
    * @param listener The listener
    * @exception JMSException Thrown if an error occurs
    */
   public void setMessageListener(MessageListener listener) throws JMSException
   {
      session.lock();
      try
      {
         checkState();
         session.checkStrict();
         if (listener == null)
            consumer.setMessageListener(null);
         else
            consumer.setMessageListener(wrapMessageListener(listener));
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Get message selector
    * @return The selector
    * @exception JMSException Thrown if an error occurs
    */
   public String getMessageSelector() throws JMSException
   {
      if (trace)
         log.trace("getMessageSelector()");

      checkState();
      return consumer.getMessageSelector();
   }
   
   /**
    * Receive
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public Message receive() throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("receive " + this);

         checkState();
         Message message = consumer.receive();

         if (trace)
            log.trace("received " + this + " result=" + message);

         if (message == null)
            return null;
         else
            return wrapMessage(message);
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Receive
    * @param timeout The timeout value
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public Message receive(long timeout) throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("receive " + this + " timeout=" + timeout);

         checkState();
         Message message = consumer.receive(timeout);

         if (trace)
            log.trace("received " + this + " result=" + message);

         if (message == null)
            return null;
         else
            return wrapMessage(message);
      }
      finally
      {
         session.unlock();
      }
   }

   /**
    * Receive
    * @return The message
    * @exception JMSException Thrown if an error occurs
    */
   public Message receiveNoWait() throws JMSException
   {
      session.lock();
      try
      {
         if (trace)
            log.trace("receiveNoWait " + this);

         checkState();
         Message message = consumer.receiveNoWait();

         if (trace)
            log.trace("received " + this + " result=" + message);

         if (message == null)
            return null;
         else
            return wrapMessage(message);
      }
      finally
      {
         session.unlock();
      }
   }
   
   /**
    * Close consumer
    * @exception JMSException Thrown if an error occurs
    */
   void closeConsumer() throws JMSException
   {
      if (trace)
         log.trace("closeConsumer()");

      consumer.close();
   }
   
   /**
    * Wrap message
    * @param message The message to be wrapped
    * @return The wrapped message
    */
   Message wrapMessage(Message message)
   {
      if (trace)
         log.trace("wrapMessage(" + message + ")");

      if (message instanceof BytesMessage)
         return new JBMBytesMessage((BytesMessage) message, session);
      else if (message instanceof MapMessage)
         return new JBMMapMessage((MapMessage) message, session);
      else if (message instanceof ObjectMessage)
         return new JBMObjectMessage((ObjectMessage) message, session);
      else if (message instanceof StreamMessage)
         return new JBMStreamMessage((StreamMessage) message, session);
      else if (message instanceof TextMessage)
         return new JBMTextMessage((TextMessage) message, session);
      return new JBMMessage(message, session);
   }
   
   /**
    * Wrap message listener
    * @param listener The listener to be wrapped
    * @return The wrapped listener
    */
   MessageListener wrapMessageListener(MessageListener listener)
   {
      if (trace)
         log.trace("getMessageSelector()");

      return new JBMMessageListener(listener, this);
   }
}
