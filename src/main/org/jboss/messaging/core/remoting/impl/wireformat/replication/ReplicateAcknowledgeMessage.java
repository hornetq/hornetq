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

package org.jboss.messaging.core.remoting.impl.wireformat.replication;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A ReplicateAcknowledgeMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Mar 2009 18:36:30
 *
 *
 */
public class ReplicateAcknowledgeMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
      
   //TODO - use queue id not name for smaller packet size
   private SimpleString uniqueName;
   
   private long messageID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicateAcknowledgeMessage(final SimpleString uniqueName, final long messageID)
   {
      super(REPLICATE_ACKNOWLEDGE);

      this.uniqueName = uniqueName;
      
      this.messageID = messageID;
   }

   // Public --------------------------------------------------------

   public ReplicateAcknowledgeMessage()
   {
      super(REPLICATE_ACKNOWLEDGE);
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeSimpleString(uniqueName);
      buffer.writeLong(messageID);
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      uniqueName = buffer.readSimpleString();
      messageID = buffer.readLong();
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }
   
   public long getMessageID()
   {
      return messageID;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
