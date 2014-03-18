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
package org.hornetq.core.server.impl;

import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.PageSubscriptionCounter;
import org.hornetq.core.paging.impl.Page;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.PageCountPending;
import org.hornetq.core.persistence.impl.journal.AddMessageRecord;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.impl.LocalQueueBinding;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;

public class PostOfficeJournalLoader implements JournalLoader
{
   protected final PostOffice postOffice;
   protected final PagingManager pagingManager;
   private StorageManager storageManager;
   private final QueueFactory queueFactory;
   protected final NodeManager nodeManager;
   private final ManagementService managementService;
   private final GroupingHandler groupingHandler;
   private Configuration configuration;
   private Map<Long, Queue> queues;

   public PostOfficeJournalLoader(PostOffice postOffice,
                                  PagingManager pagingManager,
                                  StorageManager storageManager,
                                  QueueFactory queueFactory,
                                  NodeManager nodeManager,
                                  ManagementService managementService,
                                  GroupingHandler groupingHandler,
                                  Configuration configuration)
   {

      this.postOffice = postOffice;
      this.pagingManager = pagingManager;
      this.storageManager = storageManager;
      this.queueFactory = queueFactory;
      this.nodeManager = nodeManager;
      this.managementService = managementService;
      this.groupingHandler = groupingHandler;
      this.configuration = configuration;
      queues = new HashMap<>();
   }

   public PostOfficeJournalLoader(PostOffice postOffice,
                                  PagingManager pagingManager,
                                  StorageManager storageManager,
                                  QueueFactory queueFactory,
                                  NodeManager nodeManager,
                                  ManagementService managementService,
                                  GroupingHandler groupingHandler,
                                  Configuration configuration,
                                  Map<Long, Queue> queues)
   {

      this(postOffice, pagingManager, storageManager, queueFactory, nodeManager, managementService, groupingHandler, configuration);
      this.queues = queues;
   }

   @Override
   public void initQueues(Map<Long, QueueBindingInfo> queueBindingInfosMap, List<QueueBindingInfo> queueBindingInfos) throws Exception
   {
      int duplicateID = 0;
      for (QueueBindingInfo queueBindingInfo : queueBindingInfos)
      {
         queueBindingInfosMap.put(queueBindingInfo.getId(), queueBindingInfo);

         Filter filter = FilterImpl.createFilter(queueBindingInfo.getFilterString());

         boolean isTopicIdentification =
            filter != null && filter.getFilterString() != null &&
               filter.getFilterString().toString().equals(HornetQServerImpl.GENERIC_IGNORED_FILTER);

         if (postOffice.getBinding(queueBindingInfo.getQueueName()) != null)
         {

            if (isTopicIdentification)
            {
               long tx = storageManager.generateUniqueID();
               storageManager.deleteQueueBinding(tx, queueBindingInfo.getId());
               storageManager.commitBindings(tx);
               continue;
            }
            else
            {

               SimpleString newName = queueBindingInfo.getQueueName().concat("-" + (duplicateID++));
               HornetQServerLogger.LOGGER.queueDuplicatedRenaming(queueBindingInfo.getQueueName().toString(), newName.toString());
               queueBindingInfo.replaceQueueName(newName);
            }
         }

         PageSubscription subscription = null;

         if (!isTopicIdentification)
         {
            subscription = pagingManager.getPageStore(queueBindingInfo.getAddress())
               .getCursorProvider()
               .createSubscription(queueBindingInfo.getId(), filter, true);
         }

         Queue queue = queueFactory.createQueue(queueBindingInfo.getId(),
                                                queueBindingInfo.getAddress(),
                                                queueBindingInfo.getQueueName(),
                                                filter,
                                                subscription,
                                                true,
                                                false);

         Binding binding = new LocalQueueBinding(queueBindingInfo.getAddress(), queue, nodeManager.getNodeId());

         queues.put(queueBindingInfo.getId(), queue);

         postOffice.addBinding(binding);

         managementService.registerAddress(queueBindingInfo.getAddress());
         managementService.registerQueue(queue, queueBindingInfo.getAddress(), storageManager);

      }
   }

   public void handleAddMessage(Map<Long, Map<Long, AddMessageRecord>> queueMap) throws Exception
   {
      for (Map.Entry<Long, Map<Long, AddMessageRecord>> entry : queueMap.entrySet())
      {
         long queueID = entry.getKey();

         Map<Long, AddMessageRecord> queueRecords = entry.getValue();

         Queue queue = this.queues.get(queueID);

         if (queue == null)
         {
            if (queueRecords.values().size() != 0)
            {
               HornetQServerLogger.LOGGER.journalCannotFindQueueForMessage(queueID);
            }

            continue;
         }

         // Redistribution could install a Redistributor while we are still loading records, what will be an issue with
         // prepared ACKs
         // We make sure te Queue is paused before we reroute values.
         queue.pause();

         Collection<AddMessageRecord> valueRecords = queueRecords.values();

         long currentTime = System.currentTimeMillis();

         for (AddMessageRecord record : valueRecords)
         {
            long scheduledDeliveryTime = record.getScheduledDeliveryTime();

            if (scheduledDeliveryTime != 0 && scheduledDeliveryTime <= currentTime)
            {
               scheduledDeliveryTime = 0;
               record.getMessage().removeProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            }

            if (scheduledDeliveryTime != 0)
            {
               record.getMessage().putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, scheduledDeliveryTime);
            }

            MessageReference ref = postOffice.reroute(record.getMessage(), queue, null);

            ref.setDeliveryCount(record.getDeliveryCount());

            if (scheduledDeliveryTime != 0)
            {
               record.getMessage().removeProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            }
         }
      }
   }

   public void handleNoMessageReferences(Map<Long, ServerMessage> messages)
   {
      for (ServerMessage msg : messages.values())
      {
         if (msg.getRefCount() == 0)
         {
            HornetQServerLogger.LOGGER.journalUnreferencedMessage(msg.getMessageID());
            try
            {
               storageManager.deleteMessage(msg.getMessageID());
            }
            catch (Exception ignored)
            {
               HornetQServerLogger.LOGGER.journalErrorDeletingMessage(ignored, msg.getMessageID());
            }
         }
      }
   }

   @Override
   public void handleGroupingBindings(List<GroupingInfo> groupingInfos)
   {
      for (GroupingInfo groupingInfo : groupingInfos)
      {
         if (groupingHandler != null)
         {
            groupingHandler.addGroupBinding(new GroupBinding(groupingInfo.getId(),
                                                             groupingInfo.getGroupId(),
                                                             groupingInfo.getClusterName()));
         }
      }
   }

   @Override
   public void handleDuplicateIds(Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
      for (Map.Entry<SimpleString, List<Pair<byte[], Long>>> entry : duplicateIDMap.entrySet())
      {
         SimpleString address = entry.getKey();

         DuplicateIDCache cache = postOffice.getDuplicateIDCache(address);

         if (configuration.isPersistIDCache())
         {
            cache.load(entry.getValue());
         }
      }
   }

   @Override
   public void postLoad(Journal messageJournal, ResourceManager resourceManager, Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
      for (Queue queue : queues.values())
      {
         queue.resume();
      }

      if (System.getProperty("org.hornetq.opt.directblast") != null)
      {
         messageJournal.runDirectJournalBlast();
      }
   }

   @Override
   public void handlePreparedSendMessage(ServerMessage message, Transaction tx, long queueID) throws Exception
   {
      Queue queue = queues.get(queueID);

      if (queue == null)
      {
         HornetQServerLogger.LOGGER.journalMessageInPreparedTX(queueID);
         return;
      }
      postOffice.reroute(message, queue, tx);
   }

   @Override
   public void handlePreparedAcknowledge(long messageID, List<MessageReference> referencesToAck, long queueID) throws Exception
   {
      Queue queue = queues.get(queueID);

      if (queue == null)
      {
         throw new IllegalStateException("Cannot find queue with id " + queueID);
      }

      MessageReference removed = queue.removeReferenceWithID(messageID);

      if (removed == null)
      {
         HornetQServerLogger.LOGGER.journalErrorRemovingRef(messageID);
      }
      else
      {
         referencesToAck.add(removed);
      }
   }

   @Override
   public void handlePreparedTransaction(Transaction tx, List<MessageReference> referencesToAck, Xid xid, ResourceManager resourceManager) throws Exception
   {
      for (MessageReference ack : referencesToAck)
      {
         ack.getQueue().reacknowledge(tx, ack);
      }

      tx.setState(Transaction.State.PREPARED);

      resourceManager.putTransaction(xid, tx);
   }

   /**
    * This method will recover the counters after failures making sure the page counter doesn't get out of sync
    *
    * @param pendingNonTXPageCounter
    * @throws Exception
    */
   public void recoverPendingPageCounters(List<PageCountPending> pendingNonTXPageCounter) throws Exception
   {
      // We need a structure of the following
      // Address -> PageID -> QueueID -> List<PageCountPending>
      // The following loop will sort the records according to the hierarchy we need

      Transaction txRecoverCounter = new TransactionImpl(storageManager);

      Map<SimpleString, Map<Long, Map<Long, List<PageCountPending>>>> perAddressMap = generateMapsOnPendingCount(queues, pendingNonTXPageCounter, txRecoverCounter);

      for (SimpleString address : perAddressMap.keySet())
      {
         PagingStore store = pagingManager.getPageStore(address);
         Map<Long, Map<Long, List<PageCountPending>>> perPageMap = perAddressMap.get(address);

         // We have already generated this before, so it can't be null
         assert (perPageMap != null);

         for (Long pageId : perPageMap.keySet())
         {
            Map<Long, List<PageCountPending>> perQueue = perPageMap.get(pageId);

            // This can't be true!
            assert (perQueue != null);

            if (store.checkPageFileExists(pageId.intValue()))
            {
               // on this case we need to recalculate the records
               Page pg = store.createPage(pageId.intValue());
               pg.open();

               List<PagedMessage> pgMessages = pg.read(storageManager);
               Map<Long, AtomicInteger> countsPerQueueOnPage = new HashMap<Long, AtomicInteger>();

               for (PagedMessage pgd : pgMessages)
               {
                  if (pgd.getTransactionID() <= 0)
                  {
                     for (long q : pgd.getQueueIDs())
                     {
                        AtomicInteger countQ = countsPerQueueOnPage.get(q);
                        if (countQ == null)
                        {
                           countQ = new AtomicInteger(0);
                           countsPerQueueOnPage.put(q, countQ);
                        }
                        countQ.incrementAndGet();
                     }
                  }
               }

               for (Map.Entry<Long, List<PageCountPending>> entry : perQueue.entrySet())
               {
                  for (PageCountPending record : entry.getValue())
                  {
                     HornetQServerLogger.LOGGER.debug("Deleting pg tempCount " + record.getID());
                     storageManager.deletePendingPageCounter(txRecoverCounter.getID(), record.getID());
                  }

                  PageSubscriptionCounter counter = store.getCursorProvider().getSubscription(entry.getKey()).getCounter();

                  AtomicInteger value = countsPerQueueOnPage.get(entry.getKey());

                  if (value == null)
                  {
                     HornetQServerLogger.LOGGER.debug("Page " + entry.getKey() + " wasn't open, so we will just ignore");
                  }
                  else
                  {
                     HornetQServerLogger.LOGGER.debug("Replacing counter " + value.get());
                     counter.increment(txRecoverCounter, value.get());
                  }
               }
            }
            else
            {
               // on this case the page file didn't exist, we just remove all the records since the page is already gone
               HornetQServerLogger.LOGGER.debug("Page " + pageId + " didn't exist on address " + address + ", so we are just removing records");
               for (List<PageCountPending> records : perQueue.values())
               {
                  for (PageCountPending record : records)
                  {
                     HornetQServerLogger.LOGGER.debug("Removing pending page counter " + record.getID());
                     storageManager.deletePendingPageCounter(txRecoverCounter.getID(), record.getID());
                     txRecoverCounter.setContainsPersistent();
                  }
               }
            }
         }
      }

      txRecoverCounter.commit();
   }

   @Override
   public void cleanUp()
   {
      queues.clear();
   }

   /**
    * This generates a map for use on the recalculation and recovery of pending maps after reloading it
    *
    * @param queues
    * @param pendingNonTXPageCounter
    * @param txRecoverCounter
    * @return
    * @throws Exception
    */
   private Map<SimpleString, Map<Long, Map<Long, List<PageCountPending>>>>
   generateMapsOnPendingCount(Map<Long, Queue> queues, List<PageCountPending>
      pendingNonTXPageCounter, Transaction txRecoverCounter) throws Exception
   {
      Map<SimpleString, Map<Long, Map<Long, List<PageCountPending>>>> perAddressMap = new HashMap<SimpleString, Map<Long, Map<Long, List<PageCountPending>>>>();
      for (PageCountPending pgCount : pendingNonTXPageCounter)
      {
         long queueID = pgCount.getQueueID();
         long pageID = pgCount.getPageID();

         // We first figure what Queue is based on the queue id
         Queue queue = queues.get(queueID);

         if (queue == null)
         {
            HornetQServerLogger.LOGGER.debug("removing pending page counter id = " + pgCount.getID() + " as queueID=" + pgCount.getID() + " no longer exists");
            // this means the queue doesn't exist any longer, we will remove it from the storage
            storageManager.deletePendingPageCounter(txRecoverCounter.getID(), pgCount.getID());
            txRecoverCounter.setContainsPersistent();
            continue;
         }

         // Level 1 on the structure, per address
         SimpleString address = queue.getAddress();

         Map<Long, Map<Long, List<PageCountPending>>> perPageMap = perAddressMap.get(address);

         if (perPageMap == null)
         {
            perPageMap = new HashMap();
            perAddressMap.put(address, perPageMap);
         }


         Map<Long, List<PageCountPending>> perQueueMap = perPageMap.get(pageID);

         if (perQueueMap == null)
         {
            perQueueMap = new HashMap();
            perPageMap.put(pageID, perQueueMap);
         }

         List<PageCountPending> pendingCounters = perQueueMap.get(queueID);

         if (pendingCounters == null)
         {
            pendingCounters = new LinkedList<PageCountPending>();
            perQueueMap.put(queueID, pendingCounters);
         }

         pendingCounters.add(pgCount);

         perQueueMap.put(queueID, pendingCounters);
      }
      return perAddressMap;
   }
}
