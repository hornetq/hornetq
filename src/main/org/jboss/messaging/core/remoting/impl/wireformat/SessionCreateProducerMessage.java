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
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateProducerMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------


   private int maxRate;

   private boolean autoGroupId;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerMessage(final int maxRate, final boolean autoGroupId)
   {
      super(SESS_CREATEPRODUCER);

      this.maxRate = maxRate;

      this.autoGroupId = autoGroupId;
   }

   public SessionCreateProducerMessage()
   {
      super(SESS_CREATEPRODUCER);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", maxrate=" + maxRate);
      buff.append(", autoGroupId=" + autoGroupId);
      buff.append("]");
      return buff.toString();
   }

   public int getMaxRate()
   {
      return maxRate;
   }

   public boolean isAutoGroupId()
   {
      return autoGroupId;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(maxRate);
      buffer.putBoolean(autoGroupId);
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      maxRate = buffer.getInt();
      autoGroupId = buffer.getBoolean();
   }

   public boolean equals(Object other)
   {
      if (other instanceof SessionCreateProducerMessage == false)
      {
         return false;
      }

      SessionCreateProducerMessage r = (SessionCreateProducerMessage)other;

      return super.equals(other) && this.autoGroupId == autoGroupId;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
