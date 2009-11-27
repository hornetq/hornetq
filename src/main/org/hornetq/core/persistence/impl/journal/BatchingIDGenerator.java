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

package org.hornetq.core.persistence.impl.journal;

import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.IDGenerator;

/**
 * A BatchingIDGenerator
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <mailto:tim.fox@jboss.org">Tim Fox</a>
 *
 *
 */
public class BatchingIDGenerator implements IDGenerator
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(BatchingIDGenerator.class);


   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private final AtomicLong counter;

   private final Journal journalStorage;

   private final long checkpointSize;

   private volatile long nextID;

   public BatchingIDGenerator(final long start, final long checkpointSize, final Journal journalstorage)
   {
      counter = new AtomicLong(start);

      // as soon as you generate the first ID, the nextID should be updated
      nextID = start;

      this.checkpointSize = checkpointSize;

      
      this.journalStorage = journalstorage;
   }
   
   public void close()
   {
      storeID(counter.incrementAndGet(), counter.get());
   }

   public void loadState(final long journalID, final HornetQBuffer buffer)
   {
      IDCounterEncoding encoding = new IDCounterEncoding();

      encoding.decode(buffer);

      // Keep nextID and counter the same, the next generateID will update the checkpoint
      nextID = encoding.id;
      
      counter.set(nextID);
   }

   public long generateID()
   {
      long id = counter.getAndIncrement();

      if (id >= nextID)
      {
         saveCheckPoint(id);

         return id;
      }
      else
      {
         return id;
      }
   }
   
   public long getCurrentID()
   {
      return counter.get();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private synchronized void saveCheckPoint(final long id)
   {
      if (id >= nextID)
      {
         nextID += checkpointSize;
         storeID(counter.incrementAndGet(), nextID);
      }
   }

   
   private void storeID(final long journalID, final long id)
   {
      try
      {
         journalStorage.appendAddRecord(journalID, JournalStorageManager.ID_COUNTER_RECORD, new IDCounterEncoding(id), true);
      }
      catch (Exception e)
      {
         log.error("Failed to store id", e);
      }
   }


   // Inner classes -------------------------------------------------

   private static final class IDCounterEncoding implements EncodingSupport
   {
      long id;

      IDCounterEncoding(final long id)
      {
         this.id = id;
      }

      IDCounterEncoding()
      {
      }

      public void decode(final HornetQBuffer buffer)
      {
         id = buffer.readLong();
      }

      public void encode(final HornetQBuffer buffer)
      {
         buffer.writeLong(id);
      }

      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG;
      }

   }

}
