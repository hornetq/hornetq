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
 * A ReplicationAddMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationDeleteMessage extends PacketImpl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long id;

   /** 0 - Bindings, 1 - MessagesJournal */
   private byte journalID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationDeleteMessage()
   {
      super(REPLICATION_DELETE);
   }

   public ReplicationDeleteMessage(final byte journalID, final long id)
   {
      this();
      this.journalID = journalID;
      this.id = id;
   }

   // Public --------------------------------------------------------

   @Override
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_BYTE + DataConstants.SIZE_LONG;

   }

   @Override
   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeByte(journalID);
      buffer.writeLong(id);
   }

   @Override
   public void decodeBody(final HornetQBuffer buffer)
   {
      journalID = buffer.readByte();
      id = buffer.readLong();
   }

   /**
    * @return the id
    */
   public long getId()
   {
      return id;
   }

   /**
    * @return the journalID
    */
   public byte getJournalID()
   {
      return journalID;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
