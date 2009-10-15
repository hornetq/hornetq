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

package org.hornetq.core.journal;

import java.util.List;

import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.server.HornetQComponent;

/**
 * 
 * A Journal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface Journal extends HornetQComponent
{
   // Non transactional operations

   void appendAddRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception;

   void appendAddRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception;

   void appendUpdateRecord(long id, byte recordType, byte[] record, boolean sync) throws Exception;

   void appendUpdateRecord(long id, byte recordType, EncodingSupport record, boolean sync) throws Exception;

   void appendDeleteRecord(long id, boolean sync) throws Exception;

   // Transactional operations

   void appendAddRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception;

   void appendAddRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception;

   void appendUpdateRecordTransactional(long txID, long id, byte recordType, byte[] record) throws Exception;

   void appendUpdateRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception;

   void appendDeleteRecordTransactional(long txID, long id, byte[] record) throws Exception;

   void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception;

   void appendDeleteRecordTransactional(long txID, long id) throws Exception;

   void appendCommitRecord(long txID, boolean sync) throws Exception;

   /** 
    * 
    * <p>If the system crashed after a prepare was called, it should store information that is required to bring the transaction 
    *     back to a state it could be committed. </p>
    * 
    * <p> transactionData allows you to store any other supporting user-data related to the transaction</p>
    * 
    * @param txID
    * @param transactionData - extra user data for the prepare
    * @throws Exception
    */
   void appendPrepareRecord(long txID, EncodingSupport transactionData, boolean sync) throws Exception;

   void appendPrepareRecord(long txID, byte[] transactionData, boolean sync) throws Exception;

   void appendRollbackRecord(long txID, boolean sync) throws Exception;

   // Load
   
   long load(LoaderCallback reloadManager) throws Exception;


   long load(List<RecordInfo> committedRecords, List<PreparedTransactionInfo> preparedTransactions, TransactionFailureCallback transactionFailure) throws Exception;

   int getAlignment() throws Exception;

   void perfBlast(int pages) throws Exception;


}
