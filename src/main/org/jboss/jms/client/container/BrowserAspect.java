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
package org.jboss.jms.client.container;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.jms.delegate.BrowserDelegate;

/**
 * 
 * Aspect that caches blocks of messages during queue browsing 
 * in the aspect thus preventing excessive network traffic.
 * 
 * This aspect is PER_INSTANCE.
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 */
public class BrowserAspect
{
   // Constants -----------------------------------------------------
   
   //TODO - these need to be configurable by the user - should be configured from jboss-aop.xml
   
   //FIXME - This interceptor is currently broken
   
   private static final boolean BATCH_MESSAGES = false;

   private static final int MSG_BLOCK_SIZE = 5;
   
   // Attributes ----------------------------------------------------
   
   private Message[] cache;
   private int pos;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public Object handleNextMessage(Invocation invocation) throws Throwable
   {   
      if (!BATCH_MESSAGES) return invocation.invokeNext();
      
      checkCache(invocation);
      Message mess = cache[pos++];
      if (pos == cache.length)
      {
         cache = null;
      }
      return mess;
   }
   
   public Object handleHasNextMessage(Invocation invocation) throws Throwable
   { 
      if (cache != null)
      {
         return Boolean.TRUE;
      }
      return invocation.invokeNext();
   }
   
   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   private void checkCache(Invocation invocation) throws JMSException
   {
      if (cache == null)
      {
         BrowserDelegate bd = (BrowserDelegate)invocation.getTargetObject();
         cache = bd.nextMessageBlock(MSG_BLOCK_SIZE);
         pos = 0;
      }
   }
   
   // Inner Classes -------------------------------------------------
   
}

