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
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.core.client.HornetQClientLogger;
import org.hornetq.core.client.impl.ClientSessionInternal;

/**
 * HornetQ implementation of a JMS MessageConsumer.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 */
public final class HornetQMessageConsumer implements QueueReceiver, TopicSubscriber
{
   // Constants -----------------------------------------------------

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

   private boolean closed = false;

   // Constructors --------------------------------------------------

   protected HornetQMessageConsumer(final HornetQSession session,
                                 final ClientConsumer consumer,
                                 final boolean noLocal,
                                 final HornetQDestination destination,
                                 final String selector,
                                 final SimpleString autoDeleteQueueName) throws JMSException
   {
      this.session = session;

      this.consumer = consumer;

      ackMode = session.getAcknowledgeMode();

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

   public void setMessageListener(final MessageListener listener) throws JMSException
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
      return getMessage(0, false);
   }

   public Message receive(final long timeout) throws JMSException
   {
      return getMessage(timeout, false);
   }

   public Message receiveNoWait() throws JMSException
   {
      return getMessage(0, true);
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
      finally
      {
         closed = true;
      }
   }

   // QueueReceiver implementation ----------------------------------

   public Queue getQueue() throws JMSException
   {
      checkClosed();

      return (Queue)destination;
   }

   // TopicSubscriber implementation --------------------------------

   public Topic getTopic() throws JMSException
   {
      checkClosed();

      return (Topic)destination;
   }

   public boolean getNoLocal() throws JMSException
   {
      checkClosed();

      return noLocal;
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      return "HornetQMessageConsumer[" + consumer + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void checkClosed() throws JMSException
   {
      if (closed || session.getCoreSession().isClosed())
      {
         throw new IllegalStateException("Consumer is closed");
      }
   }

   private HornetQMessage getMessage(final long timeout, final boolean noWait) throws JMSException
   {
      try
      {
         ClientMessage message;

         if (noWait)
         {
            message = consumer.receiveImmediate();
         }
         else
         {
            message = consumer.receive(timeout);
         }

         HornetQMessage msg = null;

         if (message != null)
         {
            msg = HornetQMessage.createMessage(message,
                                               (ackMode == Session.CLIENT_ACKNOWLEDGE |
                                                ackMode == HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE) ?
                                                                session.getCoreSession() : null);

            try
            {
               msg.doBeforeReceive();
            }
            catch (IndexOutOfBoundsException ioob)
            {
               ((ClientSessionInternal)session.getCoreSession()).markRollbackOnly();
               // In case this exception happen you will need to know where it happened.
               // it has been a bug here in the past, and this was used to debug it.
               // nothing better than keep it for future investigations in case it happened again
               IndexOutOfBoundsException newIOOB = new IndexOutOfBoundsException(ioob.getMessage() + "@" + msg.getCoreMessage());
               newIOOB.initCause(ioob);
               HornetQClientLogger.LOGGER.warn(newIOOB.getMessage(), newIOOB);
               throw ioob;
            }

            // We Do the ack after doBeforeRecive, as in the case of large messages, this may fail so we don't want messages redelivered
            // https://issues.jboss.org/browse/JBPAPP-6110
            if (session.getAcknowledgeMode() == HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE)
            {
               msg.setIndividualAcknowledge();
            }
            else
            {
               message.acknowledge();
            }
         }

         return msg;
      }
      catch (HornetQException e)
      {
         ((ClientSessionInternal)session.getCoreSession()).markRollbackOnly();
         throw JMSExceptionHelper.convertFromHornetQException(e);
      }
   }

   // Inner classes -------------------------------------------------

}
