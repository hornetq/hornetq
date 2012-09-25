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

import javax.jms.MessageListener;
import javax.jms.Session;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.jms.HornetQJMSLogger;

/**
 *
 * A JMSMessageListenerWrapper
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JMSMessageListenerWrapper implements MessageHandler
{
   private final HornetQSession session;

   private final MessageListener listener;

   private final ClientConsumer consumer;

   private final boolean transactedOrClientAck;

   private final boolean individualACK;

   protected JMSMessageListenerWrapper(final HornetQSession session,
                                       final ClientConsumer consumer,
                                       final MessageListener listener,
                                       final int ackMode)
   {
      this.session = session;

      this.consumer = consumer;

      this.listener = listener;

      transactedOrClientAck = (ackMode == Session.SESSION_TRANSACTED || ackMode == Session.CLIENT_ACKNOWLEDGE) || session.isXA();

      individualACK = (ackMode == HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
   }

   /**
    * In this method we apply the JMS acknowledgement and redelivery semantics
    * as per JMS spec
    */
   public void onMessage(final ClientMessage message)
   {
      HornetQMessage msg = HornetQMessage.createMessage(message, session.getCoreSession());

      if (individualACK)
      {
         msg.setIndividualAcknowledge();
      }

      try
      {
         msg.doBeforeReceive();
      }
      catch (Exception e)
      {
         HornetQJMSLogger.LOGGER.errorPreparingMessageForReceipt(e);

         return;
      }

      if (transactedOrClientAck)
      {
         try
         {
            message.acknowledge();
         }
         catch (HornetQException e)
         {
            HornetQJMSLogger.LOGGER.errorProcessingMessage(e);
         }
      }

      try
      {
         listener.onMessage(msg);
      }
      catch (RuntimeException e)
      {
         // See JMS 1.1 spec, section 4.5.2

         HornetQJMSLogger.LOGGER.onMessageError(e);

         if (!transactedOrClientAck)
         {
            try
            {
               if (individualACK)
               {
                  message.individualAcknowledge();
               }

               session.getCoreSession().rollback(true);

               session.setRecoverCalled(true);
            }
            catch (Exception e2)
            {
               HornetQJMSLogger.LOGGER.errorRecoveringSession(e2);
            }
         }
      }

      if (!session.isRecoverCalled() && !individualACK)
      {
         try
         {
            // We don't want to call this if the consumer was closed from inside onMessage
            if (!consumer.isClosed() && !transactedOrClientAck)
            {
               message.acknowledge();
            }
         }
         catch (HornetQException e)
         {
            HornetQJMSLogger.LOGGER.errorProcessingMessage(e);
         }
      }

      session.setRecoverCalled(false);
   }
}
