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

package org.jboss.messaging.core.message;


/**
 * 
 * A MessageHolder.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class MessageHolder
{
   /*
    * The number of channels *currently in memory* that hold a reference to the message
    * We need this so we know when to evict the message from the store (when it reaches zero)
    * Note that we also maintain a persistent channel count on the message itself.
    * This is the total number of channels whether loaded in memory or not that hold a reference to the
    * message and is needed to know when it is safe to remove the message from the db
    */
   private int inMemoryChannelCount;
   
   private Message msg;
   
   private SimpleMessageStore ms;
   
   public MessageHolder(Message msg, SimpleMessageStore ms)
   {
      this.msg = msg;
      this.ms = ms;
   }    
   
   public synchronized void incrementInMemoryChannelCount()
   {            
      inMemoryChannelCount++;
   }
   
   public synchronized void decrementInMemoryChannelCount()
   {
      inMemoryChannelCount--;      

      if (inMemoryChannelCount == 0)
      {
         // can remove the message from the message store
         ms.forgetMessage(msg.getMessageID());
      }
   }
   
   public synchronized int getInMemoryChannelCount()
   {
      return inMemoryChannelCount;
   }
 
   public Message getMessage()
   {
      return msg;
   }   
}
