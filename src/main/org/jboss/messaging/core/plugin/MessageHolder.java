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

package org.jboss.messaging.core.plugin;

import org.jboss.messaging.core.Message;

/**
 * 
 * A MessageHolder.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * MessageHolder.java,v 1.1 2006/02/23 17:45:58 timfox Exp
 */
class MessageHolder
{
   /*
    * The number of channels that hold a reference to the message
    */
   private int channelCount;
   
   private Message msg;
   
   private SimpleMessageStore ms;
   
   public MessageHolder(Message msg, SimpleMessageStore ms)
   {
      this.msg = msg;
      this.ms = ms;
   }    
   
   public synchronized void incrementChannelCount()
   {
      channelCount++;
   }
   
   public synchronized void decrementChannelCount()
   {
      channelCount--;      
      
      if (channelCount == 0)
      {
         // can remove the message from the message store
         ms.forgetMessage(msg.getMessageID());
      }
   }
   
   public synchronized int getChannelCount()
   {
      return channelCount;
   }
 
   public Message getMessage()
   {
      return msg;
   }   
}
