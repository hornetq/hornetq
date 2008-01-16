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
import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

import org.jboss.jms.client.api.ClientBrowser;
import org.jboss.jms.message.JBossMessage;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class JBossQueueBrowser implements QueueBrowser, Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(JBossQueueBrowser.class);

   
   private static final long serialVersionUID = 4245650830082712281L;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ClientBrowser delegate;
   private Queue queue;
   private String messageSelector;

   // Constructors ---------------------------------------------------------------------------------

   JBossQueueBrowser(Queue queue, String messageSelector, ClientBrowser delegate)
   {
      this.delegate = delegate;
      this.queue = queue;
      this.messageSelector = messageSelector;
   }

   // QueueBrowser implementation -------------------------------------------------------------------

   public void close() throws JMSException
   {
      delegate.closing(-1);
      delegate.close();
   }

   public Enumeration getEnumeration() throws JMSException
   {
      delegate.reset();
      return new BrowserEnumeration();
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
      return "JBossQueueBrowser->" + delegate;
   }

   public ClientBrowser getDelegate()
   {
      return delegate;
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
            
            return delegate.hasNextMessage();
         }
         catch (JMSException e)
         {
            throw new IllegalStateException(e.getMessage());
         }
      }

      public Object nextElement()
      {
         try
         {
            Message message = delegate.nextMessage();

            JBossMessage jbm = JBossMessage.createMessage(message, 0, 0);
            
            jbm.doBeforeReceive();                        
            
            return jbm;
         }
         catch (Exception e)
         {
            e.printStackTrace();
            throw new IllegalStateException(e.getMessage());
         }
      }
   }
}
