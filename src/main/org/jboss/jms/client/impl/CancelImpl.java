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
package org.jboss.jms.client.impl;


/**
 * 
 * A Cancel.
 * 
 * Used to send a cancel (NACK) to the server
 * 
 * When we cancel we send delivery count info, this means delivery count
 * can be updated on the server, and into storage if persistent, and the
 * message can be sent to DLQ if appropriate
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 3047 $</tt>
 *
 * $Id: CancelImpl.java 3047 2007-08-23 19:02:05Z clebert.suconic@jboss.com $
 *
 */
public class CancelImpl implements Cancel
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private long deliveryId;
   
   private int deliveryCount;    
   
   private boolean expired;
   
   private boolean reachedMaxDeliveryAttempts;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public CancelImpl()
   {      
   }
   
   public CancelImpl(long deliveryId, int deliveryCount, boolean expired, boolean maxDeliveries)
   {      
      this.deliveryId = deliveryId;
      
      this.deliveryCount = deliveryCount;
      
      this.expired = expired;
      
      this.reachedMaxDeliveryAttempts = maxDeliveries;
   }

   // Public --------------------------------------------------------
   
   public long getDeliveryId()
   {
      return deliveryId;
   }
   
   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public boolean isExpired()
   {
      return expired;
   }
   
   public boolean isReachedMaxDeliveryAttempts()
   {
      return reachedMaxDeliveryAttempts;
   }

   public String toString()
   {
      return "CancelImpl(deliveryId=" + deliveryId +
             ", deliveryCount=" + deliveryCount +
             ", expired=" + expired +
             ", reachedMaxDeliveryAttempts=" + reachedMaxDeliveryAttempts + ")";
   }

   // Class YYY overrides -------------------------------------------

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner Classes -------------------------------------------------
   
}

