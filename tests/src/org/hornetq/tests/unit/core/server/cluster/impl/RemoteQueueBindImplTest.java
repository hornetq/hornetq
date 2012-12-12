/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.unit.core.server.cluster.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.impl.RemoteQueueBindingImpl;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.LinkedListIterator;

/**
 * A RemoteQueueBindImplTest
 *
 * @author clebertsuconic
 *
 *
 */
public class RemoteQueueBindImplTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAddRemoveConsumer() throws Exception
   {

      final long id = RandomUtil.randomLong();
      final SimpleString address = RandomUtil.randomSimpleString();
      final SimpleString uniqueName = RandomUtil.randomSimpleString();
      final SimpleString routingName = RandomUtil.randomSimpleString();
      final Long remoteQueueID = RandomUtil.randomLong();
      final SimpleString filterString = new SimpleString("A>B");
      final Queue storeAndForwardQueue = new FakeQueue();
      final SimpleString bridgeName = RandomUtil.randomSimpleString();
      final int distance = 0;
      RemoteQueueBindingImpl binding = new RemoteQueueBindingImpl(id,
                                                                  address,
                                                                  uniqueName,
                                                                  routingName,
                                                                  remoteQueueID,
                                                                  filterString,
                                                                  storeAndForwardQueue,
                                                                  bridgeName,
                                                                  distance);

      for (int i = 0; i < 100; i++)
      {
         binding.addConsumer(new SimpleString("B" + i + "<A"));
      }

      assertEquals(100, binding.getFilters().size());

      for (int i = 0; i < 100; i++)
      {
         binding.removeConsumer(new SimpleString("B" + i + "<A"));
      }

      assertEquals(0, binding.getFilters().size());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class FakeQueue implements Queue
   {

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#isInternalQueue()
       */
      public boolean isInternalQueue()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#setInternalQueue(boolean)
       */
      public void setInternalQueue(boolean internalQueue)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Bindable#route(org.hornetq.core.server.ServerMessage, org.hornetq.core.server.RoutingContext)
       */
      public void route(ServerMessage message, RoutingContext context) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getName()
       */
      public SimpleString getName()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getID()
       */
      public long getID()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getFilter()
       */
      public Filter getFilter()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getPageSubscription()
       */
      public PageSubscription getPageSubscription()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#isDurable()
       */
      public boolean isDurable()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#isTemporary()
       */
      public boolean isTemporary()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addConsumer(org.hornetq.core.server.Consumer)
       */
      public void addConsumer(Consumer consumer) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#removeConsumer(org.hornetq.core.server.Consumer)
       */
      public void removeConsumer(Consumer consumer) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getConsumerCount()
       */
      public int getConsumerCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#reload(org.hornetq.core.server.MessageReference)
       */
      public void reload(MessageReference ref)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addTail(org.hornetq.core.server.MessageReference)
       */
      public void addTail(MessageReference ref)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addTail(org.hornetq.core.server.MessageReference, boolean)
       */
      public void addTail(MessageReference ref, boolean direct)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addHead(org.hornetq.core.server.MessageReference)
       */
      public void addHead(MessageReference ref)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#acknowledge(org.hornetq.core.server.MessageReference)
       */
      public void acknowledge(MessageReference ref) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#acknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
       */
      public void acknowledge(Transaction tx, MessageReference ref) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#reacknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
       */
      public void reacknowledge(Transaction tx, MessageReference ref) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
       */
      public void cancel(Transaction tx, MessageReference ref)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.server.MessageReference, long)
       */
      public void cancel(MessageReference reference, long timeBase) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deliverAsync()
       */
      public void deliverAsync()
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getMessageCount()
       */
      public long getMessageCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getDeliveringCount()
       */
      public int getDeliveringCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#referenceHandled()
       */
      public void referenceHandled()
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getScheduledCount()
       */
      public int getScheduledCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getScheduledMessages()
       */
      public List<MessageReference> getScheduledMessages()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getMessagesAdded()
       */
      public long getMessagesAdded()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#removeReferenceWithID(long)
       */
      public MessageReference removeReferenceWithID(long id) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getReference(long)
       */
      public MessageReference getReference(long id)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deleteAllReferences()
       */
      public int deleteAllReferences() throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deleteReference(long)
       */
      public boolean deleteReference(long messageID) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deleteMatchingReferences(org.hornetq.core.filter.Filter)
       */
      public int deleteMatchingReferences(Filter filter) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#expireReference(long)
       */
      public boolean expireReference(long messageID) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#expireReferences(org.hornetq.core.filter.Filter)
       */
      public int expireReferences(Filter filter) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#expireReferences()
       */
      public void expireReferences() throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#expire(org.hornetq.core.server.MessageReference)
       */
      public void expire(MessageReference ref) throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#sendMessageToDeadLetterAddress(long)
       */
      public boolean sendMessageToDeadLetterAddress(long messageID) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#sendMessagesToDeadLetterAddress(org.hornetq.core.filter.Filter)
       */
      public int sendMessagesToDeadLetterAddress(Filter filter) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#changeReferencePriority(long, byte)
       */
      public boolean changeReferencePriority(long messageID, byte newPriority) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#changeReferencesPriority(org.hornetq.core.filter.Filter, byte)
       */
      public int changeReferencesPriority(Filter filter, byte newPriority) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#moveReference(long, org.hornetq.api.core.SimpleString)
       */
      public boolean moveReference(long messageID, SimpleString toAddress) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#moveReference(long, org.hornetq.api.core.SimpleString, boolean)
       */
      public boolean moveReference(long messageID, SimpleString toAddress, boolean rejectDuplicates) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#moveReferences(org.hornetq.core.filter.Filter, org.hornetq.api.core.SimpleString)
       */
      public int moveReferences(Filter filter, SimpleString toAddress) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#moveReferences(org.hornetq.core.filter.Filter, org.hornetq.api.core.SimpleString, boolean)
       */
      public int moveReferences(Filter filter, SimpleString toAddress, boolean rejectDuplicates) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addRedistributor(long)
       */
      public void addRedistributor(long delay)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#cancelRedistributor()
       */
      public void cancelRedistributor() throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#hasMatchingConsumer(org.hornetq.core.server.ServerMessage)
       */
      public boolean hasMatchingConsumer(ServerMessage message)
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getConsumers()
       */
      public Collection<Consumer> getConsumers()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#checkRedelivery(org.hornetq.core.server.MessageReference, long)
       */
      public boolean checkRedelivery(MessageReference ref, long timeBase) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#iterator()
       */
      public LinkedListIterator<MessageReference> iterator()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#setExpiryAddress(org.hornetq.api.core.SimpleString)
       */
      public void setExpiryAddress(SimpleString expiryAddress)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#pause()
       */
      public void pause()
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#resume()
       */
      public void resume()
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#isPaused()
       */
      public boolean isPaused()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getExecutor()
       */
      public Executor getExecutor()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#resetAllIterators()
       */
      public void resetAllIterators()
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#blockOnExecutorFuture()
       */
      public boolean flushExecutor()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#close()
       */
      public void close() throws Exception
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#isDirectDeliver()
       */
      public boolean isDirectDeliver()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getAddress()
       */
      public SimpleString getAddress()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#forceDelivery()
       */
      public void forceDelivery()
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getInstantMessageCount()
       */
      public long getInstantMessageCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getInstantMessagesAdded()
       */
      public long getInstantMessagesAdded()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addHead(java.util.LinkedList)
       */
      public void addHead(LinkedList<MessageReference> refs)
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#destroyPaging()
       */
      public void destroyPaging()
      {
         // TODO Auto-generated method stub

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getDeliveringMessages()
       */
      @Override
      public Map<String, List<MessageReference>> getDeliveringMessages()
      {
         // TODO Auto-generated method stub
         return null;
      }

   }

}
