/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.client;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.jboss.jms.client.api.ClientConsumer;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.api.MessageHandler;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.util.Logger;

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

   private ClientConsumer consumer;
   
   private MessageListener listener;
   
   private MessageHandler coreListener;
   
   private JBossSession session;
   
   private int ackMode;

   // Constructors --------------------------------------------------

   public JBossMessageConsumer(JBossSession session, ClientConsumer consumer) throws JMSException
   {      
      this.session = session;
      
      this.consumer = consumer;
      
      this.ackMode = session.getAcknowledgeMode();
   }

   // MessageConsumer implementation --------------------------------

   public String getMessageSelector() throws JMSException
   {
      return consumer.getMessageSelector();
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
      
      consumer.setMessageHandler(coreListener);
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
      consumer.closing();
      consumer.close();
   }

   // QueueReceiver implementation ----------------------------------

   public Queue getQueue() throws JMSException
   {
      return (Queue)JBossDestination.fromCoreDestination(consumer.getDestination());
   }

   // TopicSubscriber implementation --------------------------------

   public Topic getTopic() throws JMSException
   {
      return (Topic)JBossDestination.fromCoreDestination(consumer.getDestination());
   }


   public boolean getNoLocal() throws JMSException
   {
      return consumer.getNoLocal();
   }

   public ClientConsumer getDelegate()
   {
       return consumer;
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
      org.jboss.messaging.core.Message message =  consumer.receive(timeout);
            
      JBossMessage jbm = null;
      
      if (message != null)
      {         
         //At this point JMS considers the message delivered
         session.getCoreSession().delivered();
                  
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

   // Inner classes -------------------------------------------------

}
