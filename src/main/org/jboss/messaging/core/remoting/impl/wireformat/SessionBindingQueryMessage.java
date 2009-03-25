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
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A SessionQueueQueryMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryMessage extends PacketImpl
{
   private SimpleString address;

   public SessionBindingQueryMessage(final SimpleString address)
   {
      super(SESS_BINDINGQUERY);

      this.address = address;
   }

   public SessionBindingQueryMessage()
   {
      super(SESS_BINDINGQUERY);
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + address.sizeof();
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeSimpleString(address);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      address = buffer.readSimpleString();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionBindingQueryMessage == false)
      {
         return false;
      }

      SessionBindingQueryMessage r = (SessionBindingQueryMessage)other;

      return super.equals(other) && address.equals(r.address);
   }

}
