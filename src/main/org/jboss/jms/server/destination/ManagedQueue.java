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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.selector.Selector;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.Queue;

/**
 * A ManagedQueue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ManagedQueue extends ManagedDestination
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ManagedQueue.class);

   // Static ---------------------------------------------------------------------------------------

   private static boolean trace = log.isTraceEnabled();

   // Attributes -----------------------------------------------------------------------------------
   
   private MessageCounter messageCounter;
   
   private Queue queue;

   // Constructors ---------------------------------------------------------------------------------

   public ManagedQueue()
   {
   }

   public ManagedQueue(String name, int fullSize, int pageSize, int downCacheSize, boolean clustered)
   {
      super(name, fullSize, pageSize, downCacheSize, clustered);
   }

   // ManagedDestination overrides -----------------------------------------------------------------

   public boolean isQueue()
   {
      return true;
   }
   
   public void setMessageCounterHistoryDayLimit(int limit) throws Exception
   {
      super.setMessageCounterHistoryDayLimit(limit);
      
      if (messageCounter != null)
      {
         messageCounter.setHistoryLimit(limit);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public int getMessageCount() throws Exception
   {
      int count = queue.getMessageCount();

      if (trace) { log.trace(this + " returning MessageCount = " + count); }

      return count;
   }
   
   public int getScheduledMessageCount() throws Exception
   {     
      int count = queue.getScheduledCount();

      if (trace) { log.trace(this + " returning ScheduledMessageCount = " + count); }

      return count;
   }
   
   public int getConsumersCount() throws Exception
   {
      int count = queue.getLocalDistributor().getNumberOfReceivers();

      if (trace) { log.trace(this + " returning ConsumersCount = " + count); }

      return count;
   }

   public void removeAllMessages() throws Throwable
   {
      queue.removeAllReferences();
   }

   public List listAllMessages(String selector) throws Exception
   {
      return this.listMessages(ALL, selector);
   }
   
   public List listDurableMessages(String selector) throws Exception
   {
      return this.listMessages(DURABLE, selector);
   }
   
   public List listNonDurableMessages(String selector) throws Exception
   {
      return this.listMessages(NON_DURABLE, selector);
   }
   
   private List listMessages(int type, String selector) throws Exception
   {
      Selector sel = null;
                        
      if (selector != null && "".equals(selector.trim()))
      {
         selector = null;
      }
      
      if (selector != null)
      {        
         sel = new Selector(selector);
      }
      
      List msgs = new ArrayList();
      
      List allMsgs = queue.browse(sel);
      
      Iterator iter = allMsgs.iterator();
      
      while (iter.hasNext())
      {
         Message msg = (Message)iter.next();
         
         if (type == ALL || (type == DURABLE && msg.isReliable()) || (type == NON_DURABLE && !msg.isReliable()))
         {
            msgs.add(msg);
         }
      }
      
      return msgs;
   }
   
   public MessageCounter getMessageCounter()
   {
      return messageCounter;
   }
   
   public void setMessageCounter(MessageCounter counter)
   {
      this.messageCounter = counter;
   }
   
   public void setQueue(Queue queue)
   {
      this.queue = queue;
   }
   
   public Queue getQueue()
   {
      return queue;
   }
   
   public String toString()
   {
      return "ManagedQueue[" + name + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
