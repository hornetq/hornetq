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
package org.jboss.messaging.core.contract;

/**
 * A reference to a message.
 * 
 * Channels store message references rather than the messages themselves.
 * 
 * If many channels have contain the same reference this makes a lot of sense
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface MessageReference
{      
   long getPagingOrder();
   
   void setPagingOrder(long order);   
   
   void releaseMemoryReference();
   
   MessageReference copy();
   
   Message getMessage();
   
   /**
    * 
    * @return The time in the future that delivery will be delayed until, or zero if
    * no scheduled delivery will occur
    */
   long getScheduledDeliveryTime();
   
   void setScheduledDeliveryTime(long scheduledDeliveryTime);
   
   /**
    * @return the number of times delivery has been attempted for this routable
    */
   int getDeliveryCount();
   
   void setDeliveryCount(int deliveryCount);     
}
