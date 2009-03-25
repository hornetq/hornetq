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
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionCreateConsumerMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;

   private SimpleString filterString;

   private boolean browseOnly;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerMessage(final SimpleString queueName,
                                       final SimpleString filterString,
                                       final boolean browseOnly)
   {
      super(SESS_CREATECONSUMER);

      this.queueName = queueName;
      this.filterString = filterString;
      this.browseOnly = browseOnly;
   }

   public SessionCreateConsumerMessage()
   {
      super(SESS_CREATECONSUMER);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append("]");
      return buff.toString();
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public boolean isBrowseOnly()
   {
      return browseOnly;
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + queueName.sizeof() +
             SimpleString.sizeofNullableString(filterString) +
             DataConstants.SIZE_BOOLEAN;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(browseOnly);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      queueName = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();
      browseOnly = buffer.readBoolean();
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof SessionCreateConsumerMessage == false)
      {
         return false;
      }

      SessionCreateConsumerMessage r = (SessionCreateConsumerMessage)other;

      return super.equals(other) && queueName.equals(r.queueName) && filterString == null ? r.filterString == null
                                                                                         : filterString.equals(r.filterString);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
