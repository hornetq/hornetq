/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.tests.unit.core.server.impl.fakes;

import javax.transaction.xa.Xid;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.impl.PageCountPending;
import org.hornetq.core.persistence.impl.journal.AddMessageRecord;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.JournalLoader;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;

public class FakeJournalLoader implements JournalLoader
{
   @Override
   public void handleNoMessageReferences(Map<Long, ServerMessage> messages)
   {
   }

   @Override
   public void handleAddMessage(Map<Long, Map<Long, AddMessageRecord>> queueMap) throws Exception
   {
   }

   @Override
   public void initQueues(Map<Long, QueueBindingInfo> queueBindingInfosMap, List<QueueBindingInfo> queueBindingInfos) throws Exception
   {
   }

   @Override
   public void handleGroupingBindings(List<GroupingInfo> groupingInfos)
   {
   }

   @Override
   public void handleDuplicateIds(Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
   }

   @Override
   public void postLoad(Journal messageJournal, ResourceManager resourceManager, Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap)
   {
   }

   @Override
   public void handlePreparedSendMessage(ServerMessage message, Transaction tx, long queueID)
   {
   }

   @Override
   public void handlePreparedAcknowledge(long messageID, List<MessageReference> referencesToAck, long queueID)
   {
   }

   @Override
   public void handlePreparedTransaction(Transaction tx, List<MessageReference> referencesToAck, Xid xid, ResourceManager resourceManager)
   {
   }

   @Override
   public void recoverPendingPageCounters(List<PageCountPending> pendingNonTXPageCounter) throws Exception
   {
   }

   @Override
   public void cleanUp()
   {

   }
}
