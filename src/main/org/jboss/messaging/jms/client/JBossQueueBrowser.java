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

import java.util.Enumeration;
import java.util.NoSuchElementException;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         <p/>
 *         $Id$
 */
public class JBossQueueBrowser implements QueueBrowser
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossQueueBrowser.class);

   private static final long NEXT_MESSAGE_TIMEOUT = 1000;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ClientSession session;

   private ClientConsumer consumer;

   private JBossQueue queue;

   private SimpleString filterString;

   // Constructors ---------------------------------------------------------------------------------

   public JBossQueueBrowser(JBossQueue queue, String messageSelector, ClientSession session) throws JMSException
   {
      this.session = session;
      this.queue = queue;
      if (messageSelector != null)
      {
         this.filterString = new SimpleString(SelectorTranslator.convertToJBMFilterString(messageSelector));
      }
   }

   // QueueBrowser implementation -------------------------------------------------------------------

   public void close() throws JMSException
   {
      if (consumer != null)
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
   }

   public Enumeration getEnumeration() throws JMSException
   {
      try
      {
         close();

         consumer = session.createConsumer(queue.getSimpleAddress(), filterString, true);

         return new BrowserEnumeration();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);
      }

   }

   public String getMessageSelector() throws JMSException
   {
      return filterString == null ? null : filterString.toString();
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
      ClientMessage current = null;

      public boolean hasMoreElements()
      {
         if (current == null)
         {
            try
            {
               // todo change this to consumer.receiveImmediate() once
               // https://jira.jboss.org/jira/browse/JBMESSAGING-1432 is completed
               current = consumer.receive(NEXT_MESSAGE_TIMEOUT);
            }
            catch (MessagingException e)
            {
               return false;
            }
         }
         return current != null;
      }

      public Object nextElement()
      {
         JBossMessage jbm;
         if (hasMoreElements())
         {
            ClientMessage next = current;
            current = null;
            jbm = JBossMessage.createMessage(next, session);
            try
            {
               jbm.doBeforeReceive();
            }
            catch (Exception e)
            {
               log.error("Failed to create message", e);

               return null;
            }
            return jbm;
         }
         else
         {
            throw new NoSuchElementException();
         }
      }
   }
}
