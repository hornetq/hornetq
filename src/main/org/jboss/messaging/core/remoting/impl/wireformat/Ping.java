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
 * 
 * A Ping
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Ping extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long connectionTTL;

   private long clientFailureCheckPeriod;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public Ping(final long clientFailureCheckPeriod, final long connectionTTL)
   {
      super(PING);

      this.connectionTTL = connectionTTL;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public Ping()
   {
      super(PING);
   }

   // Public --------------------------------------------------------

   public boolean isWriteAlways()
   {
      return true;
   }
   
   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + 2 * DataConstants.SIZE_LONG;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeLong(connectionTTL);
      buffer.writeLong(clientFailureCheckPeriod);
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      connectionTTL = buffer.readLong();
      clientFailureCheckPeriod = buffer.readLong();
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", connectionTTL=" + connectionTTL);
      buf.append(", clientFailureCheckPeriod=" + clientFailureCheckPeriod);
      buf.append("]");
      return buf.toString();
   }

   public boolean equals(Object other)
   {
      if (other instanceof Ping == false)
      {
         return false;
      }

      Ping r = (Ping)other;

      return super.equals(other) && this.connectionTTL == r.connectionTTL &&
             this.clientFailureCheckPeriod == r.clientFailureCheckPeriod;
   }

   public final boolean isRequiresConfirmations()
   {
      return false;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
