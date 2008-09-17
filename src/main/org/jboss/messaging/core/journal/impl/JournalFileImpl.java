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

package org.jboss.messaging.core.journal.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A JournalFileImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class JournalFileImpl implements JournalFile
{
   private static final Logger log = Logger.getLogger(JournalFileImpl.class);

   private final SequentialFile file;

   private final int orderingID;

   private int offset;

   private final AtomicInteger posCount = new AtomicInteger(0);

   private boolean canReclaim;

   private final Map<JournalFile, AtomicInteger> negCounts = new ConcurrentHashMap<JournalFile, AtomicInteger>();

   public JournalFileImpl(final SequentialFile file, final int orderingID)
   {
      this.file = file;

      this.orderingID = orderingID;
   }

   public int getPosCount()
   {
      return posCount.intValue();
   }

   public boolean isCanReclaim()
   {
      return canReclaim;
   }

   public void setCanReclaim(final boolean canReclaim)
   {
      this.canReclaim = canReclaim;
   }

   public void incNegCount(final JournalFile file)
   {
      getOrCreateNegCount(file).incrementAndGet();
   }

   public int getNegCount(final JournalFile file)
   {
      AtomicInteger count = negCounts.get(file);

      if (count == null)
      {
         return 0;
      }
      else
      {
         return count.intValue();
      }
   }

   public void incPosCount()
   {
      posCount.incrementAndGet();
   }

   public void decPosCount()
   {
      posCount.decrementAndGet();
   }

   public void extendOffset(final int delta)
   {
      offset += delta;
   }

   public int getOffset()
   {
      return offset;
   }

   public int getOrderingID()
   {
      return orderingID;
   }

   public void setOffset(final int offset)
   {
      this.offset = offset;
   }

   public SequentialFile getFile()
   {
      return file;
   }

   @Override
   public String toString()
   {
      try
      {
         return "JournalFileImpl: " + file.getFileName();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         return "Error:" + e.toString();
      }
   }

   /** Receive debug information about the journal */
   public String debug()
   {
      StringBuilder builder = new StringBuilder();

      for (Entry<JournalFile, AtomicInteger> entry : negCounts.entrySet())
      {
         builder.append(" file = " + entry.getKey() + " negcount value = " + entry.getValue() + "\n");
      }

      return builder.toString();
   }

   private synchronized AtomicInteger getOrCreateNegCount(final JournalFile file)
   {
      AtomicInteger count = negCounts.get(file);

      if (count == null)
      {
         count = new AtomicInteger();
         negCounts.put(file, count);
      }

      return count;
   }

}
