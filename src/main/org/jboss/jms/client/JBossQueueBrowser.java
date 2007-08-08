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

import org.jboss.jms.delegate.BrowserDelegate;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class JBossQueueBrowser implements QueueBrowser, Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 4245650830082712281L;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private BrowserDelegate delegate;
   private Queue queue;
   private String messageSelector;

   // Constructors ---------------------------------------------------------------------------------

   JBossQueueBrowser(Queue queue, String messageSelector, BrowserDelegate delegate)
   {
      this.delegate = delegate;
      this.queue = queue;
      this.messageSelector = messageSelector;
   }

   // QueueBrowser implementation -------------------------------------------------------------------

   public void close() throws JMSException
   {
      delegate.closing();
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

   public BrowserDelegate getDelegate()
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
            return delegate.nextMessage();
         }
         catch (JMSException e)
         {
            throw new IllegalStateException(e.getMessage());
         }
      }
   }
}
