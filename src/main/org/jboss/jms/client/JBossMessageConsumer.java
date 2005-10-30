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

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.jboss.jms.delegate.ConsumerDelegate;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class JBossMessageConsumer implements MessageConsumer, QueueReceiver, TopicSubscriber, Serializable
{   
   // Constants -----------------------------------------------------  
   
   private static final long serialVersionUID = -8776908463975467851L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConsumerDelegate delegate;

   // Constructors --------------------------------------------------

   public JBossMessageConsumer(ConsumerDelegate delegate)
   {      
      this.delegate = delegate;
   }

   // MessageConsumer implementation --------------------------------

   public String getMessageSelector() throws JMSException
   {
      //return (String)delegate.getMetaData(JMSAdvisor.SELECTOR);
      return delegate.getMessageSelector();
   }


   public MessageListener getMessageListener() throws JMSException
   {
      return delegate.getMessageListener();
   }


   public void setMessageListener(MessageListener listener) throws JMSException
   {
      delegate.setMessageListener(listener);
   }


   public Message receive() throws JMSException
   {
      return delegate.receive(0);
   }


   public Message receive(long timeout) throws JMSException
   {
      return delegate.receive(timeout);
   }


   public Message receiveNoWait() throws JMSException
   {
      return delegate.receive(-1);
   }


   public void close() throws JMSException
   {
      delegate.closing();
      delegate.close();
   }


   // QueueReceiver implementation ----------------------------------


   public Queue getQueue() throws JMSException
   {
      return (Queue)delegate.getDestination();
   }

   // TopicSubscriber implementation --------------------------------


   public Topic getTopic() throws JMSException
   {
      return (Topic)delegate.getDestination();
   }


   public boolean getNoLocal() throws JMSException
   {
      return delegate.getNoLocal();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
