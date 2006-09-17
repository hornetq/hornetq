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
package org.jboss.jms.server.destination;

import java.util.List;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;

import org.jboss.jms.selector.Selector;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.plugin.postoffice.Binding;

/**
 * A ManagedQueue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ManagedQueue extends ManagedDestination
{
   public ManagedQueue()
   {      
   }
   
   public ManagedQueue(String name, int fullSize, int pageSize, int downCacheSize)
   {
      super(name, fullSize, pageSize, downCacheSize);
   }

   public int getMessageCount() throws Exception
   {
      Binding binding = (Binding)postOffice.listBindingsForCondition(name).iterator().next();
      
      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding for queue:" + name);
      }
      
      Queue queue = binding.getQueue();

      return queue.messageCount();
   }
   
   public void removeAllMessages() throws Throwable
   {
      Binding binding = (Binding)postOffice.listBindingsForCondition(name).iterator().next();
      
      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding for queue:" + name);
      }
      
      Queue queue = binding.getQueue();

      queue.removeAllReferences();
   }
   
   public List listMessages(String selector) throws Exception
   {
      if (selector != null)
      {
         selector = selector.trim();
         if (selector.equals(""))
         {
            selector = null;
         }
      }
      
      Binding binding = (Binding)postOffice.listBindingsForCondition(name).iterator().next();
      
      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding for queue:" + name);
      }
      
      Queue queue = binding.getQueue();

      try 
      {
         List msgs;
         if (selector == null)
         {
            msgs = queue.browse();
         }
         else
         {
            msgs = queue.browse(new Selector(selector));
         }
         return msgs;
      }
      catch (InvalidSelectorException e)
      {
         Throwable th = new JMSException(e.getMessage());
         th.initCause(e);
         throw (JMSException)th;
      }
   }

   public boolean isQueue()
   {
      return true;
   }
}
