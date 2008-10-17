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
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SessionCreateBrowserMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;
   
   private SimpleString filterString;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateBrowserMessage(final SimpleString queueName, final SimpleString filterString)
   {
      super(SESS_CREATEBROWSER);

      this.queueName = queueName;
      this.filterString = filterString;
   }
   
   public SessionCreateBrowserMessage()
   {
      super(SESS_CREATEBROWSER);
   }
   
   // Public --------------------------------------------------------

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putSimpleString(queueName);
      buffer.putNullableSimpleString(filterString);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      queueName = buffer.getSimpleString();
      filterString = buffer.getNullableSimpleString();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", queueName=" + queueName + ", filterString="
            + filterString + "]";
   }
   
   public boolean equals(Object other)
   {
      if (other instanceof SessionCreateBrowserMessage == false)
      {
         return false;
      }
            
      SessionCreateBrowserMessage r = (SessionCreateBrowserMessage)other;
      
      return super.equals(other) && this.queueName.equals(r.queueName) &&
             this.filterString == null ? r.filterString == null : this.filterString.equals(r.filterString);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
