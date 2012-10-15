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

import org.hornetq.core.journal.RecordInfo;

/**
 * A JournalReader
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface JournalReaderCallback
{
   void onReadAddRecord(RecordInfo info) throws Exception;

   /**
    * @param recordInfo
    * @throws Exception
    */
   void onReadUpdateRecord(RecordInfo recordInfo) throws Exception;

   /**
    * @param recordID
    */
   void onReadDeleteRecord(long recordID) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    * @throws Exception
    */
   void onReadAddRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    * @throws Exception
    */
   void onReadUpdateRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    */
   void onReadDeleteRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param extraData
    * @param numberOfRecords
    */
   void onReadPrepareRecord(long transactionID, byte[] extraData, int numberOfRecords) throws Exception;

   /**
    * @param transactionID
    * @param numberOfRecords
    */
   void onReadCommitRecord(long transactionID, int numberOfRecords) throws Exception;

   /**
    * @param transactionID
    */
   void onReadRollbackRecord(long transactionID) throws Exception;

   public void markAsDataFile(JournalFile file);

}
