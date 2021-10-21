/*
 * Copyright 2005-2014 Red Hat, Inc.
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

import java.util.Enumeration;
import java.util.NoSuchElementException;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;

/**
 * HornetQ implementation of a JMS QueueBrowser.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *
 */
public final class HornetQQueueBrowser implements QueueBrowser
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private final ConnectionFactoryOptions options;

   private final ClientSession session;

   private ClientConsumer consumer;

   private final HornetQQueue queue;

   private SimpleString filterString;

   // Constructors ---------------------------------------------------------------------------------

   protected HornetQQueueBrowser(final ConnectionFactoryOptions options, final HornetQQueue queue, final String messageSelector, final ClientSession session) throws JMSException
   {
      this.options = options;
      this.session = session;
      this.queue = queue;
      if (messageSelector != null)
      {
         filterString = new SimpleString(SelectorTranslator.convertToHornetQFilterString(messageSelector));
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
         catch (HornetQException e)
         {
            throw JMSExceptionHelper.convertFromHornetQException(e);
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
      catch (HornetQException e)
      {
         throw JMSExceptionHelper.convertFromHornetQException(e);
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

   @Override
   public String toString()
   {
      return "HornetQQueueBrowser->" + consumer;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private final class BrowserEnumeration implements Enumeration<HornetQMessage>
   {
      ClientMessage current = null;

      public boolean hasMoreElements()
      {
         if (current == null)
         {
            try
            {
               current = consumer.receiveImmediate();
            }
            catch (HornetQException e)
            {
               return false;
            }
         }
         return current != null;
      }

      public HornetQMessage nextElement()
      {
         HornetQMessage msg;
         if (hasMoreElements())
         {
            ClientMessage next = current;
            current = null;
            msg = HornetQMessage.createMessage(next, session, options);
            try
            {
               msg.doBeforeReceive();
            }
            catch (Exception e)
            {
               HornetQJMSClientLogger.LOGGER.errorCreatingMessage(e);

               return null;
            }
            return msg;
         }
         else
         {
            throw new NoSuchElementException();
         }
      }
   }
}
