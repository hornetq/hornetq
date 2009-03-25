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
import org.jboss.messaging.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionXAResponseMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean error;

   private int responseCode;

   private String message;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionXAResponseMessage(final boolean isError, final int responseCode, final String message)
   {
      super(SESS_XA_RESP);

      error = isError;

      this.responseCode = responseCode;

      this.message = message;
   }

   public SessionXAResponseMessage()
   {
      super(SESS_XA_RESP);
   }

   // Public --------------------------------------------------------

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_BOOLEAN +
             DataConstants.SIZE_INT +
             nullableStringEncodeSize(message);
   }

   @Override
   public boolean isResponse()
   {
      return true;
   }

   public boolean isError()
   {
      return error;
   }

   public int getResponseCode()
   {
      return responseCode;
   }

   public String getMessage()
   {
      return message;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeBoolean(error);
      buffer.writeInt(responseCode);
      buffer.writeNullableString(message);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      error = buffer.readBoolean();
      responseCode = buffer.readInt();
      message = buffer.readNullableString();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionXAResponseMessage == false)
      {
         return false;
      }

      SessionXAResponseMessage r = (SessionXAResponseMessage)other;

      return super.equals(other) && error == r.error && responseCode == r.responseCode && message.equals(r.message);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
