/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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
import org.jboss.messaging.util.DataConstants;

/**
 * A RollbackMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Feb 18, 2009 2:11:17 PM
 *
 *
 */
public class RollbackMessage extends PacketImpl
{

   /**
    * @param type
    */
   public RollbackMessage()
   {
      super(SESS_ROLLBACK);
   }

   public RollbackMessage(final boolean isLastMessageAsDelived)
   {
      super(SESS_ROLLBACK);
      this.isLastMessageAsDelived = isLastMessageAsDelived;
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean isLastMessageAsDelived;

   /**
    * @return the isLastMessageAsDelived
    */
   public boolean isLastMessageAsDelived()
   {
      return isLastMessageAsDelived;
   }

   /**
    * @param isLastMessageAsDelived the isLastMessageAsDelived to set
    */
   public void setLastMessageAsDelived(final boolean isLastMessageAsDelived)
   {
      this.isLastMessageAsDelived = isLastMessageAsDelived;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl#getRequiredBufferSize()
    */
   @Override
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_BOOLEAN;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putBoolean(isLastMessageAsDelived);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      this.isLastMessageAsDelived = buffer.getBoolean();
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
