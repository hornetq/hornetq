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

package org.hornetq.tests.unit.core.postoffice.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.LinkedListIterator;

/**
 * A FakeQueue
 *
 * @author tim
 *
 *
 */
public class FakeQueue implements Queue
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

   PageSubscription subs;

   public boolean isDirectDeliver()
   {
      // TODO Auto-generated method stub
      return false;
   }

   public void close()
   {
      // TODO Auto-generated method stub

   }

   public void forceCheckQueueSize()
   {
      // TODO Auto-generated method stub

   }

   public void reload(MessageReference ref)
   {
      // TODO Auto-generated method stub

   }

   public boolean flushExecutor()
   {
      return true;
   }

   public void addHead(MessageReference ref)
   {
      // TODO Auto-generated method stub

   }

   public void addTail(MessageReference ref, boolean direct)
   {
      // TODO Auto-generated method stub

   }

   public void addTail(MessageReference ref)
   {
      // TODO Auto-generated method stub

   }

   public void resetAllIterators()
   {
      // TODO Auto-generated method stub

   }

   private final SimpleString name;

   private final long id;

   public FakeQueue(final SimpleString name)
   {
      this(name, 0);
   }

   public FakeQueue(final SimpleString name, final long id)
   {
      this.name = name;
      this.id = id;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#acknowledge(org.hornetq.core.server.MessageReference)
    */
   public void acknowledge(final MessageReference ref) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#acknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
    */
   public void acknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addConsumer(org.hornetq.core.server.Consumer)
    */
   public void addConsumer(final Consumer consumer) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addFirst(org.hornetq.core.server.MessageReference)
    */
   public void addFirst(final MessageReference ref)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addLast(org.hornetq.core.server.MessageReference)
    */
   public void addLast(final MessageReference ref)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addRedistributor(long)
    */
   public void addRedistributor(final long delay)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.server.MessageReference)
    */
   public void cancel(final MessageReference reference, final long timeBase) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
    */
   public void cancel(final Transaction tx, final MessageReference ref)
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
    * @see org.hornetq.core.server.Queue#changeReferencePriority(long, byte)
    */
   public boolean changeReferencePriority(final long messageID, final byte newPriority) throws Exception
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
    * @see org.hornetq.core.server.Queue#checkDLQ(org.hornetq.core.server.MessageReference)
    */
   public boolean checkRedelivery(final MessageReference ref, final long timeBase) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
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
    * @see org.hornetq.core.server.Queue#deleteMatchingReferences(org.hornetq.core.filter.Filter)
    */
   public int deleteMatchingReferences(final Filter filter) throws Exception
   {
      // TODO Auto-generated method stub
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#deleteReference(long)
    */
   public boolean deleteReference(final long messageID) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#deliverAsync()
    */
   public void deliverAsync()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#deliverNow()
    */
   public void deliverNow()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#expire(org.hornetq.core.server.MessageReference)
    */
   public void expire(final MessageReference ref) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#expireReference(long)
    */
   public boolean expireReference(final long messageID) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#expireReferences()
    */
   public void expireReferences() throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#expireReferences(org.hornetq.core.filter.Filter)
    */
   public int expireReferences(final Filter filter) throws Exception
   {
      // TODO Auto-generated method stub
      return 0;
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
    * @see org.hornetq.core.server.Queue#getConsumers()
    */
   public Set<Consumer> getConsumers()
   {
      // TODO Auto-generated method stub
      return null;
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
    * @see org.hornetq.core.server.Queue#getFilter()
    */
   public Filter getFilter()
   {
      // TODO Auto-generated method stub
      return null;
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
    * @see org.hornetq.core.server.Queue#getMessagesAdded()
    */
   public long getMessagesAdded()
   {
      // TODO Auto-generated method stub
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getName()
    */
   public SimpleString getName()
   {
      return name;
   }

   public SimpleString getAddress()
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getID()
    */
   public long getID()
   {
      return id;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getReference(long)
    */
   public MessageReference getReference(final long id)
   {
      // TODO Auto-generated method stub
      return null;
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
    * @see org.hornetq.core.server.Queue#isDurable()
    */
   public boolean isDurable()
   {
      // TODO Auto-generated method stub
      return false;
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
    * @see org.hornetq.core.server.Queue#isTemporary()
    */
   public boolean isTemporary()
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
    * @see org.hornetq.core.server.Queue#list(org.hornetq.core.filter.Filter)
    */
   public List<MessageReference> list(final Filter filter)
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#lockDelivery()
    */
   public void lockDelivery()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#moveReference(long, org.hornetq.utils.SimpleString)
    */
   public boolean moveReference(final long messageID, final SimpleString toAddress) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#moveReferences(org.hornetq.core.filter.Filter, org.hornetq.utils.SimpleString)
    */
   public int moveReferences(final Filter filter, final SimpleString toAddress) throws Exception
   {
      // TODO Auto-generated method stub
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#pause()
    */
   public void pause()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#reacknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
    */
   public void reacknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#referenceHandled()
    */
   public void referenceHandled()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#removeConsumer(org.hornetq.core.server.Consumer)
    */
   public void removeConsumer(final Consumer consumer) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#removeFirstReference(long)
    */
   public MessageReference removeFirstReference(final long id) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#removeReferenceWithID(long)
    */
   public MessageReference removeReferenceWithID(final long id) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#resume()
    */
   public void resume()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#sendMessageToDeadLetterAddress(long)
    */
   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
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
    * @see org.hornetq.core.server.Queue#setExpiryAddress(org.hornetq.utils.SimpleString)
    */
   public void setExpiryAddress(final SimpleString expiryAddress)
   {
      // TODO Auto-generated method stub

   }

   // TODO Auto-generated method stub

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#unlockDelivery()
    */
   public void unlockDelivery()
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Bindable#route(org.hornetq.core.server.ServerMessage, org.hornetq.core.server.RoutingContext)
    */
   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      // TODO Auto-generated method stub

   }

   public boolean hasMatchingConsumer(final ServerMessage message)
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#checkDLQ(org.hornetq.core.server.MessageReference, java.util.concurrent.Executor)
    */
   public boolean checkDLQ(final MessageReference ref, final Executor ioExecutor) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   public Executor getExecutor()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public void addLast(MessageReference ref, boolean direct)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getPageSubscription()
    */
   public PageSubscription getPageSubscription()
   {
      return subs;
   }

   public void setPageSubscription(PageSubscription sub)
   {
      this.subs = sub;
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
    * @see org.hornetq.core.server.Queue#moveReferences(org.hornetq.core.filter.Filter, org.hornetq.api.core.SimpleString, boolean)
    */
   public int moveReferences(Filter filter, SimpleString toAddress, boolean rejectDuplicates) throws Exception
   {
      // TODO Auto-generated method stub
      return 0;
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