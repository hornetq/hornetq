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

package org.hornetq.core.persistence.impl.nullpm;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;

/**
 * A NullStorageLargeServerMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 30-Sep-08 1:51:42 PM
 *
 *
 */
public class NullStorageLargeServerMessage extends ServerMessageImpl implements LargeServerMessage
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public NullStorageLargeServerMessage()
   {
      super();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#release()
    */
   public void releaseResources()
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#addBytes(byte[])
    */
   public synchronized void addBytes(final byte[] bytes)
   {
      MessagingBuffer buffer = getBody();

      if (buffer != null)
      {
         // expand the buffer
         buffer.writeBytes(bytes);
      }
      else
      {
         // Reuse the initial byte array on the buffer construction
         setBody(ChannelBuffers.dynamicBuffer(bytes));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#deleteFile()
    */
   public void deleteFile() throws Exception
   {
      // nothing to be done here.. we don really have a file on this Storage
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#complete()
    */
   public void complete() throws Exception
   {
      // nothing to be done here.. we don really have a file on this Storage

   }

   @Override
   public boolean isLargeMessage()
   {
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#getLinkedMessage()
    */
   public LargeServerMessage getLinkedMessage()
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setLinkedMessage(org.hornetq.core.server.LargeServerMessage)
    */
   public void setLinkedMessage(LargeServerMessage message)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#isComplete()
    */
   public boolean isComplete()
   {
      // nothing to be done on null persistence
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setComplete(boolean)
    */
   public void setComplete(boolean isComplete)
   {
      // nothing to be done on null persistence
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
