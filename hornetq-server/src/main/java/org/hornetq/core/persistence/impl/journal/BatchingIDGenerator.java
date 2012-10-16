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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.IDGenerator;

/**
 * A BatchingIDGenerator
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <mailto:tim.fox@jboss.org">Tim Fox</a>
 */
public class BatchingIDGenerator implements IDGenerator
{

   private final AtomicLong counter;

   private final long checkpointSize;

   private volatile long nextID;

   private final StorageManager storageManager;

   public BatchingIDGenerator(final long start, final long checkpointSize, final StorageManager storageManager)
   {
      counter = new AtomicLong(start);

      // as soon as you generate the first ID, the nextID should be updated
      nextID = start;

      this.checkpointSize = checkpointSize;

      this.storageManager = storageManager;
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
      }
      return id;
   }

   public long getCurrentID()
   {
      return counter.get();
   }

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
         storageManager.storeID(journalID, id);
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.batchingIdError(e);
      }
   }

   public static EncodingSupport createIDEncodingSupport(final long id)
   {
      return new IDCounterEncoding(id);
   }

   // Inner classes -------------------------------------------------

   protected static final class IDCounterEncoding implements EncodingSupport
   {
      private long id;

      @Override
      public String toString()
      {
         return "IDCounterEncoding [id=" + id + "]";
      }

      private IDCounterEncoding(final long id)
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
