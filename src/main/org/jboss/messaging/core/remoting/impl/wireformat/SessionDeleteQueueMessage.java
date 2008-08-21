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

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class SessionDeleteQueueMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionDeleteQueueMessage(final SimpleString queueName)
   {
      super(SESS_DELETE_QUEUE);

      this.queueName = queueName;
   }
   
   public SessionDeleteQueueMessage()
   {
      super(SESS_DELETE_QUEUE);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append("]");
      return buff.toString();
   }
   
   public SimpleString getQueueName()
   {
      return queueName;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putSimpleString(queueName);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      queueName = buffer.getSimpleString();
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionDeleteQueueMessage == false)
      {
         return false;
      }
            
      SessionDeleteQueueMessage r = (SessionDeleteQueueMessage)other;
      
      return super.equals(other) && r.queueName.equals(this.queueName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
