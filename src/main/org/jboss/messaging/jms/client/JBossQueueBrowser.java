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

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;
import java.util.NoSuchElementException;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *
 * $Id$
 */
public class JBossQueueBrowser implements QueueBrowser
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossQueueBrowser.class);

   private static final long NEXT_MESSAGE_TIMEOUT = 5000;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ClientConsumer consumer;
   private Queue queue;
   private String messageSelector;
   private boolean firstTime = true;

   // Constructors ---------------------------------------------------------------------------------

   public JBossQueueBrowser(Queue queue, String messageSelector, ClientConsumer consumer)
   {
      this.consumer = consumer;
      this.queue = queue;
      this.messageSelector = messageSelector;
   }

   // QueueBrowser implementation -------------------------------------------------------------------

   public void close() throws JMSException
   {
      try
      {
         consumer.close();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      }
   }

   public Enumeration getEnumeration() throws JMSException
   {
      try
      {
         if(firstTime)
         {
            consumer.start();
            firstTime = false;
         }
         else
         {
            consumer.restart();
         }
         return new BrowserEnumeration();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }
   }

   public String getMessageSelector() throws JMSException
   {
      return messageSelector;
   }

   public Queue getQueue() throws JMSException
   {
      return queue;
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "JBossQueueBrowser->" + consumer;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private class BrowserEnumeration implements Enumeration
   {
      public boolean hasMoreElements()
      {
         try
         {            
            return consumer.awaitMessage(NEXT_MESSAGE_TIMEOUT);
         }
         catch (Exception e)
         {
            throw new IllegalStateException(e.getMessage());
         }
      }

      public Object nextElement()
      {
         try
         {
            if(!hasMoreElements())
            {
               throw new NoSuchElementException();  
            }

            ClientMessage message = consumer.receiveImmediate();

            if(message == null)
            {
               throw new NoSuchElementException();
            }

            JBossMessage jbm = JBossMessage.createMessage(message, null);
            
            jbm.doBeforeReceive();                        
            
            return jbm;
         }
         catch (Exception e)
         {
            throw new IllegalStateException(e.getMessage());
         }
      }
   }
}
