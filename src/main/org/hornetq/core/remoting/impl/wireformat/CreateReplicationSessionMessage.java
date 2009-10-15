/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Clebert Suconic</a>
 */
public class CreateReplicationSessionMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long sessionChannelID;

   private int windowSize;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateReplicationSessionMessage(final long sessionChannelID, final int windowSize)
   {
      super(CREATE_REPLICATION);

      this.sessionChannelID = sessionChannelID;

      this.windowSize = windowSize;
   }

   public CreateReplicationSessionMessage()
   {
      super(CREATE_REPLICATION);
   }

   // Public --------------------------------------------------------
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE +
      // buffer.writeLong(sessionChannelID);
             DataConstants.SIZE_LONG +
             // buffer.writeInt(windowSize);
             DataConstants.SIZE_INT;

   }

   @Override
   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeLong(sessionChannelID);
      buffer.writeInt(windowSize);
   }

   @Override
   public void decodeBody(final HornetQBuffer buffer)
   {
      sessionChannelID = buffer.readLong();
      windowSize = buffer.readInt();
   }

   /**
    * @return the sessionChannelID
    */
   public long getSessionChannelID()
   {
      return sessionChannelID;
   }

   /**
    * @return the windowSize
    */
   public int getWindowSize()
   {
      return windowSize;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
