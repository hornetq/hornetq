/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.remoting.impl.wireformat;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SessionBindingQueryResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryResponseMessage extends DuplicablePacket
{
   private boolean exists;
   
   private List<SimpleString> queueNames;
   
   public SessionBindingQueryResponseMessage(final boolean exists, final List<SimpleString> queueNames)
   {
      super(SESS_BINDINGQUERY_RESP);

      this.exists = exists;

      this.queueNames = queueNames;
   }
   
   public SessionBindingQueryResponseMessage()
   {
      super(SESS_BINDINGQUERY_RESP);
   }
   
   public boolean isResponse()
   {
      return true;
   }

   public boolean isExists()
   {
      return exists;
   }

   public List<SimpleString> getQueueNames()
   {
      return this.queueNames;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      super.encodeBody(buffer);
      buffer.putBoolean(exists);
      buffer.putInt(queueNames.size());      
      for (SimpleString queueName: queueNames)
      {
         buffer.putSimpleString(queueName);
      }      
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      super.decodeBody(buffer);
      exists = buffer.getBoolean();      
      int numQueues = buffer.getInt();      
      queueNames = new ArrayList<SimpleString>(numQueues);      
      for (int i = 0; i < numQueues; i++)
      {
         queueNames.add(buffer.getSimpleString());
      }          
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionBindingQueryResponseMessage == false)
      {
         return false;
      }
            
      SessionBindingQueryResponseMessage r = (SessionBindingQueryResponseMessage)other;
      
      if (super.equals(other) && this.exists == r.exists)
      {
         if (this.queueNames.size() == r.queueNames.size())
         {
            for (int i = 0; i < queueNames.size(); i++)
            {
               if (!this.queueNames.get(i).equals(r.queueNames.get(i)))
               {
                  return false;
               }
            }
         }
         else
         {
            return false;
         }
      }
      else
      {
         return false;
      }
      
      return true;
   }
   
}
