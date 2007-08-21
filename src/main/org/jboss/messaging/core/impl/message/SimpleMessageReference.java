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
package org.jboss.messaging.core.impl.message;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;

/**
 * A Simple MessageReference implementation.
 * 
 * Note that we do not need WeakReferences to message/holder objects since with the new
 * lazy loading schema we guarantee that if a message ref is in memory - it's corresponding message is
 * in memory too
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>1.3</tt>
 *
 * SimpleMessageReference.java,v 1.3 2006/02/23 17:45:57 timfox Exp
 */
public class SimpleMessageReference implements MessageReference
{   
   private static final Logger log = Logger.getLogger(SimpleMessageReference.class);
   
   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private long pagingOrder = -1;
    
   private int deliveryCount;   
   
   private long scheduledDeliveryTime;
   
   private Message message;
   
   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public SimpleMessageReference()
   {
      if (trace) { log.trace("Creating using default constructor"); }
   }

   public SimpleMessageReference(SimpleMessageReference other)
   {
      this.pagingOrder = other.pagingOrder;
      
      this.deliveryCount = other.deliveryCount;
      
      this.scheduledDeliveryTime = other.scheduledDeliveryTime;       
      
      this.message = other.message;
   }
   
   protected SimpleMessageReference(Message message)
   {
   	this.message = message;
   }   
   
   // MessageReference implementation -------------------------------
   
   public MessageReference copy()
   {
   	return new SimpleMessageReference(this);
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
   
   public long getPagingOrder()
   {
      return pagingOrder;
   }
   
   public void setPagingOrder(long order)
   {
      this.pagingOrder = order;
   }
   
   // Public --------------------------------------------------------

   public String toString()
   {
      return "Reference[" + getMessage().getMessageID() + "]:" + (getMessage().isReliable() ? "RELIABLE" : "NON-RELIABLE");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------   
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}