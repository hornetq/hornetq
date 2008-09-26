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

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossMessageConsumer implements MessageConsumer, QueueReceiver, TopicSubscriber
{   
   // Constants -----------------------------------------------------  
   
   private static final Logger log = Logger.getLogger(JBossMessageConsumer.class);
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClientConsumer consumer;
   
   private MessageListener listener;
   
   private MessageHandler coreListener;
   
   private final JBossSession session;
   
   private final int ackMode;
   
   private final boolean noLocal;
   
   private final JBossDestination destination;
   
   private final String selector;
   
   private final SimpleString autoDeleteQueueName;
     
   // Constructors --------------------------------------------------

   public JBossMessageConsumer(final JBossSession session, final ClientConsumer consumer, final boolean noLocal,
                               final JBossDestination destination, final String selector,
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
      
      coreListener = new JMSMessageListenerWrapper(session, listener, ackMode);
      
      try
      {
         consumer.setMessageHandler(coreListener);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
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
            //If non durable subscriber need to delete subscription too
            session.deleteQueue(autoDeleteQueueName);
         }
         
         session.removeConsumer(this);
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
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
      return "JBossMessageConsumer->" + consumer;
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
   
   private JBossMessage getMessage(long timeout) throws JMSException
   {
      try
      {
         ClientMessage message =  consumer.receive(timeout);
               
         JBossMessage jbm = null;
         
         if (message != null)
         {         
            message.processed();            
                     
            jbm = JBossMessage.createMessage(message, session.getCoreSession());
            
            try
            {
               jbm.doBeforeReceive();
            }
            catch (Exception e)
            {
               log.error("Failed to prepare message", e);
               
               return null;
            }
         }
         
         return jbm;
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      }
   }

   // Inner classes -------------------------------------------------

}
