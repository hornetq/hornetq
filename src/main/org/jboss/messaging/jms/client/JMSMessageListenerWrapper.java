/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.client;

import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;

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
      
   private final JBossSession session;
   
   private final MessageListener listener;
   
   private final ClientConsumer consumer;
   
   private final boolean transactedOrClientAck;
   
   public JMSMessageListenerWrapper(final JBossSession session, final ClientConsumer consumer, final MessageListener listener, final int ackMode)
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
      JBossMessage jbm = JBossMessage.createMessage(message, session.getCoreSession());
      
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
               session.getCoreSession().rollback();
               
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
            
            if (consumer.isClosed())
            {
               log.info("not acking, consumer is closed");
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
