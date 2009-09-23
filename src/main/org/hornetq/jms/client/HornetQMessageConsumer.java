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


package org.hornetq.jms.client;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.HornetQDestination;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class HornetQMessageConsumer implements MessageConsumer, QueueReceiver, TopicSubscriber
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(HornetQMessageConsumer.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClientConsumer consumer;

   private MessageListener listener;

   private MessageHandler coreListener;

   private final HornetQSession session;

   private final int ackMode;

   private final boolean noLocal;

   private final HornetQDestination destination;

   private final String selector;

   private final SimpleString autoDeleteQueueName;

   // Constructors --------------------------------------------------

   public HornetQMessageConsumer(final HornetQSession session,
                               final ClientConsumer consumer,
                               final boolean noLocal,
                               final HornetQDestination destination,
                               final String selector,
                               final SimpleString autoDeleteQueueName) throws JMSException
   {
      this.session = session;

      this.consumer = consumer;

      this.ackMode = session.getAcknowledgeMode();

      this.noLocal = noLocal;

      this.destination = destination;

      this.selector = selector;

      this.autoDeleteQueueName = autoDeleteQueueName;
   }

   // MessageConsumer implementation --------------------------------

   public String getMessageSelector() throws JMSException
   {
      checkClosed();

      return selector;
   }

   public MessageListener getMessageListener() throws JMSException
   {
      checkClosed();

      return listener;
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
      this.listener = listener;

      coreListener = listener == null ? null : new JMSMessageListenerWrapper(session, consumer, listener, ackMode);

      try
      {
         consumer.setMessageHandler(coreListener);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   public Message receive() throws JMSException
   {
      return getMessage(0);
   }

   public Message receive(long timeout) throws JMSException
   {
      return getMessage(timeout);
   }

   public Message receiveNoWait() throws JMSException
   {
      return getMessage(-1);
   }

   public void close() throws JMSException
   {
      try
      {
         consumer.close();

         if (autoDeleteQueueName != null)
         {
            // If non durable subscriber need to delete subscription too
            session.deleteQueue(autoDeleteQueueName);
         }

         session.removeConsumer(this);
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   // QueueReceiver implementation ----------------------------------

   public Queue getQueue() throws JMSException
   {
      return (Queue)destination;
   }

   // TopicSubscriber implementation --------------------------------

   public Topic getTopic() throws JMSException
   {
      return (Topic)destination;
   }

   public boolean getNoLocal() throws JMSException
   {
      return noLocal;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "HornetQRAMessageConsumer->" + consumer;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkClosed() throws JMSException
   {
      if (session.getCoreSession().isClosed())
      {
         throw new IllegalStateException("Consumer is closed");
      }
   }

   private HornetQMessage getMessage(long timeout) throws JMSException
   {
      try
      {
         ClientMessage message = consumer.receive(timeout);

         HornetQMessage msg = null;

         if (message != null)
         {
            message.acknowledge();

            msg = HornetQMessage.createMessage(message, ackMode == Session.CLIENT_ACKNOWLEDGE ? session.getCoreSession()
                                                                                           : null);

            try
            {
               msg.doBeforeReceive();
            }
            catch (Exception e)
            {
               log.error("Failed to prepare message", e);

               return null;
            }
         }

         return msg;
      }
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   // Inner classes -------------------------------------------------

}
