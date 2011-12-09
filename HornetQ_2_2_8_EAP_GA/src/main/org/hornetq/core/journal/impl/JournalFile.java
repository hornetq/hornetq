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

package org.hornetq.core.journal.impl;

import org.hornetq.core.journal.SequentialFile;

/**
 * 
 * A JournalFile
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface JournalFile
{

   /** Used during compacting (clearing counters) */
   void clearCounts();

   int getNegCount(JournalFile file);

   void incNegCount(JournalFile file);

   boolean resetNegCount(JournalFile file);

   int getPosCount();

   void incPosCount();

   void decPosCount();

   void addSize(int bytes);

   void decSize(int bytes);

   int getLiveSize();

   /** The total number of deletes this file has */
   int getTotalNegativeToOthers();

   void setCanReclaim(boolean canDelete);

   boolean isCanReclaim();

   long getOffset();

   /** This is a field to identify that records on this file actually belong to the current file.
    *  The possible implementation for this is fileID & Integer.MAX_VALUE */
   int getRecordID();

   long getFileID();

   int getJournalVersion();

   SequentialFile getFile();
}
