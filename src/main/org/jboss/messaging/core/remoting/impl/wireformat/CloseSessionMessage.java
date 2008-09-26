/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class CloseSessionMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String name;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CloseSessionMessage(final String name)
   {
      super(CLOSE_SESSION);

      this.name = name;
   }

   public CloseSessionMessage()
   {
      super(CLOSE_SESSION);
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putString(name);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      name = buffer.getString();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof CloseSessionMessage == false)
      {
         return false;
      }

      CloseSessionMessage r = (CloseSessionMessage)other;

      return super.equals(other) && name == r.name;
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
