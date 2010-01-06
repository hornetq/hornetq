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

package org.hornetq.core.replication;

import java.util.Set;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.server.HornetQComponent;

/**
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface ReplicationManager extends HornetQComponent
{
   void appendAddRecord(byte journalID, long id, byte recordType, EncodingSupport record) throws Exception;

   void appendUpdateRecord(byte journalID, long id, byte recordType, EncodingSupport record) throws Exception;

   void appendDeleteRecord(byte journalID, long id) throws Exception;

   void appendAddRecordTransactional(byte journalID, long txID, long id, byte recordType, EncodingSupport record) throws Exception;

   void appendUpdateRecordTransactional(byte journalID, long txID, long id, byte recordType, EncodingSupport record) throws Exception;

   void appendDeleteRecordTransactional(byte journalID, long txID, long id, EncodingSupport record) throws Exception;

   void appendDeleteRecordTransactional(byte journalID, long txID, long id) throws Exception;

   void appendCommitRecord(byte journalID, long txID) throws Exception;

   void appendPrepareRecord(byte journalID, long txID, EncodingSupport transactionData) throws Exception;

   void appendRollbackRecord(byte journalID, long txID) throws Exception;

   /** A list of tokens that are still waiting for replications to be completed */
   Set<OperationContext> getActiveTokens();

   /**
    * @param storeName
    * @param pageNumber
    */
   void pageClosed(SimpleString storeName, int pageNumber);

   /**
    * @param storeName
    * @param pageNumber
    */
   void pageDeleted(SimpleString storeName, int pageNumber);

   /**
    * @param message
    * @param pageNumber
    */
   void pageWrite(PagedMessage message, int pageNumber);

   void largeMessageBegin(long messageId);

   void largeMessageWrite(long messageId, byte[] body);

   void largeMessageDelete(long messageId);

   /**
    * @param journalInfo
    * @throws HornetQException 
    */
   void compareJournals(JournalLoadInformation[] journalInfo) throws HornetQException;

}
