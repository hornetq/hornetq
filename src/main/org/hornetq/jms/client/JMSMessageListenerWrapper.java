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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.logging.Logger;

/**
 * 
 * A JMSMessageListenerWrapper
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JMSMessageListenerWrapper implements MessageHandler
{
   private static final Logger log = Logger.getLogger(JMSMessageListenerWrapper.class);
      
   private final HornetQSession session;
   
   private final MessageListener listener;
   
   private final ClientConsumer consumer;
   
   private final boolean transactedOrClientAck;
   
   public JMSMessageListenerWrapper(final HornetQSession session, final ClientConsumer consumer, final MessageListener listener, final int ackMode)
   {
      this.session = session;
      
      this.consumer = consumer;
      
      this.listener = listener;
      
      this.transactedOrClientAck = ackMode == Session.SESSION_TRANSACTED || ackMode == Session.CLIENT_ACKNOWLEDGE;
   }

   /**
    * In this method we apply the JMS acknowledgement and redelivery semantics
    * as per JMS spec
    */
   public void onMessage(final ClientMessage message)
   {
      HornetQMessage jbm = HornetQMessage.createMessage(message, session.getCoreSession());
      
      try
      {
         jbm.doBeforeReceive();
      }
      catch (Exception e)
      {
         log.error("Failed to prepare message for receipt", e);
         
         return;
      }
      
      if (transactedOrClientAck)
      {
         try
         {
            message.acknowledge();
         }
         catch (MessagingException e)
         {
            log.error("Failed to process message", e);
         }
      }
      
      try
      {         
         listener.onMessage(jbm);
      }
      catch (RuntimeException e)
      {
         //See JMS 1.1 spec, section 4.5.2
         
         log.warn("Unhandled exception thrown from onMessage", e);
         
         if (!transactedOrClientAck)
         {            
            try
            {                              
               session.getCoreSession().rollback(true);
               
               session.setRecoverCalled(true);
            }
            catch (Exception e2)
            {
               log.error("Failed to recover session", e2);
            }
         }
      }            
      
      if (!session.isRecoverCalled())
      {
         try
         {
            //We don't want to call this if the consumer was closed from inside onMessage
            if (!consumer.isClosed() && !this.transactedOrClientAck)
            {
               message.acknowledge();
            }            
         }
         catch (MessagingException e)
         {
            log.error("Failed to process message", e);
         }
      }
      
      session.setRecoverCalled(false);     
   }
}
