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

package org.hornetq.core.protocol.core.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.protocol.core.PacketImpl;

/**
 * Message used to compare if the Journals between the live and
 * backup nodes are equivalent and can be used over replication.
 * The backup journal needs to be an exact copy of the live node before it starts.
 * @author <a href="mailto:tim.fox@jboss.com">Clebert Suconic</a>
 */
public class ReplicationCompareDataMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private JournalLoadInformation[] journalInformation;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationCompareDataMessage(final JournalLoadInformation[] journalInformation)
   {
      super(PacketImpl.REPLICATION_COMPARE_DATA);

      this.journalInformation = journalInformation;
   }

   public ReplicationCompareDataMessage()
   {
      super(PacketImpl.REPLICATION_COMPARE_DATA);
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeInt(journalInformation.length);
      for (JournalLoadInformation info : journalInformation)
      {
         buffer.writeInt(info.getNumberOfRecords());
         buffer.writeLong(info.getMaxID());
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      int numberOfJournals = buffer.readInt();

      journalInformation = new JournalLoadInformation[numberOfJournals];

      for (int i = 0; i < numberOfJournals; i++)
      {
         journalInformation[i] = new JournalLoadInformation();
         journalInformation[i].setNumberOfRecords(buffer.readInt());
         journalInformation[i].setMaxID(buffer.readLong());
      }
   }

   /**
    * @return the journalInformation
    */
   public JournalLoadInformation[] getJournalInformation()
   {
      return journalInformation;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
