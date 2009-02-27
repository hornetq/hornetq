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
 * A SessionContinuationMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 5, 2008 10:08:40 AM
 *
 *
 */
public abstract class SessionContinuationMessage extends PacketImpl
{

   // Constants -----------------------------------------------------

   public static final int SESSION_CONTINUATION_BASE_SIZE = BASIC_PACKET_SIZE + DataConstants.SIZE_INT +
                                                            DataConstants.SIZE_BOOLEAN;

   // Attributes ----------------------------------------------------

   protected byte[] body;

   protected boolean continues;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionContinuationMessage(byte type, final byte[] body, final boolean continues)
   {
      super(type);
      this.body = body;
      this.continues = continues;
   }

   public SessionContinuationMessage(byte type)
   {
      super(type);
   }

   // Public --------------------------------------------------------

   /**
    * @return the body
    */
   public byte[] getBody()
   {
      return body;
   }

   /**
    * @return the continues
    */
   public boolean isContinues()
   {
      return continues;
   }

   @Override
   public int getRequiredBufferSize()
   {
      return SESSION_CONTINUATION_BASE_SIZE + body.length; 
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeInt(body.length);
      buffer.writeBytes(body);
      buffer.writeBoolean(continues);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      int size = buffer.readInt();
      body = new byte[size];
      buffer.readBytes(body);
      continues = buffer.readBoolean();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
