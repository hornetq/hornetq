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
package org.jboss.messaging.core.impl;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.util.Logger;

/**
 * Implementation of a MessageReference
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>1.3</tt>
 *
 * MessageReferenceImpl.java,v 1.3 2006/02/23 17:45:57 timfox Exp
 */
public class MessageReferenceImpl implements MessageReference
{   
   private static final Logger log = Logger.getLogger(MessageReferenceImpl.class);
   
   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private int deliveryCount;   
   
   private long scheduledDeliveryTime;
   
   private Message message;
   
   private Queue queue;
   
   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public MessageReferenceImpl()
   {
      if (trace) { log.trace("Creating using default constructor"); }
   }

   public MessageReferenceImpl(MessageReferenceImpl other, Queue queue)
   {
      this.deliveryCount = other.deliveryCount;
      
      this.scheduledDeliveryTime = other.scheduledDeliveryTime;       
      
      this.message = other.message;
      
      this.queue = queue;
   }
   
   protected MessageReferenceImpl(Message message, Queue queue)
   {
   	this.message = message;
   	
   	this.queue = queue;
   }   
   
   // MessageReference implementation -------------------------------
   
   public MessageReference copy(Queue queue)
   {
   	return new MessageReferenceImpl(this, queue);
   }
   
   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public void setDeliveryCount(int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
   }
   
   public long getScheduledDeliveryTime()
   {
      return scheduledDeliveryTime;
   }

   public void setScheduledDeliveryTime(long scheduledDeliveryTime)
   {
      this.scheduledDeliveryTime = scheduledDeliveryTime;
   }
      
   public Message getMessage()
   {
      return message;
   }         
   
   public Queue getQueue()
   {
      return queue;
   }
   
   public void acknowledge(PersistenceManager persistenceManager) throws Exception
   {
      if (message.isDurable())
      {
         persistenceManager.deleteReference(this);
      }
      
      queue.decrementDeliveringCount();
   }
   
   public void cancel(PersistenceManager persistenceManager) throws Exception
   {      
      deliveryCount++;
      
      if (message.isDurable() && queue.isDurable())
      {
         persistenceManager.updateDeliveryCount(queue, this);
      }
            
      queue.decrementDeliveringCount();
   }
   
   // Public --------------------------------------------------------

   public String toString()
   {
      return "Reference[" + getMessage().getMessageID() + "]:" + (getMessage().isDurable() ? "RELIABLE" : "NON-RELIABLE");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------   
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
}