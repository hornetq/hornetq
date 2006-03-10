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
package org.jboss.jms.client.state;

import java.util.Collections;

import javax.jms.DeliveryMode;
import javax.jms.Destination;

import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.server.Version;

/**
 * State corresponding to a producer. This state is acessible inside aspects/interceptors.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ProducerState extends HierarchicalStateSupport
{
   private Destination destination;
   
   private boolean disableMessageID = false;
   
   private boolean disableMessageTimestamp = false;
   
   private int priority = 4;
   
   private long timeToLive = 0;
   
   private int deliveryMode = DeliveryMode.PERSISTENT;
   
   public ProducerState(SessionState parent, ProducerDelegate delegate, Destination dest)
   {
      super(parent, delegate);
      children = Collections.EMPTY_SET;
      this.destination = dest;
   }
     
   public Destination getDestination()
   {
      return destination;
   }
      
   public void setDestination(Destination dest)
   {
      this.destination = dest;
      
   }
   public boolean isDisableMessageID()
   {
      return disableMessageID;
   }
   
   public void setDisableMessageID(boolean disableMessageID)
   {
      this.disableMessageID = disableMessageID;
   }
   
   public boolean isDisableMessageTimestamp()
   {
      return disableMessageTimestamp;
   }
   
   public void setDisableMessageTimestamp(boolean disableMessageTimestamp)
   {
      this.disableMessageTimestamp = disableMessageTimestamp;
   }
   
   public int getPriority()
   {
      return priority;
   }
   
   public void setPriority(int priority)
   {
      this.priority = priority;
   }
   
   public long getTimeToLive()
   {
      return timeToLive;
   }
   
   public void setTimeToLive(long timeToLive)
   {
      this.timeToLive = timeToLive;
   }
   
   public int getDeliveryMode()
   {
      return deliveryMode;
   }
   
   public void setDeliveryMode(int deliveryMode)
   {
      this.deliveryMode = deliveryMode;
   }
   
   public Version getVersionToUse()
   {
      return parent.getVersionToUse();
   }
   
   
}



