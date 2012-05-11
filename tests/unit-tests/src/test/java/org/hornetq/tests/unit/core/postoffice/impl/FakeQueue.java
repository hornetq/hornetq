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

import java.util.List;
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
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#setInternalQueue(boolean)
    */
   public void setInternalQueue(boolean internalQueue)
   {
      // no-op

   }

   PageSubscription subs;

   public boolean isDirectDeliver()
   {
      // no-op
      return false;
   }

   public void close()
   {
      // no-op

   }

   public void forceCheckQueueSize()
   {
      // no-op

   }

   public void reload(MessageReference ref)
   {
      // no-op

   }

   public boolean flushExecutor()
   {
      return true;
   }

   public void addHead(MessageReference ref)
   {
      // no-op

   }

   public void addHead(List<MessageReference> ref)
   {
      // no-op

   }

   public void addTail(MessageReference ref, boolean direct)
   {
      // no-op

   }

   public void addTail(MessageReference ref)
   {
      // no-op

   }

   public void resetAllIterators()
   {
      // no-op

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
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#acknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
    */
   public void acknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addConsumer(org.hornetq.core.server.Consumer)
    */
   public void addConsumer(final Consumer consumer) throws Exception
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addFirst(org.hornetq.core.server.MessageReference)
    */
   public void addFirst(final MessageReference ref)
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addLast(org.hornetq.core.server.MessageReference)
    */
   public void addLast(final MessageReference ref)
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addRedistributor(long)
    */
   public void addRedistributor(final long delay)
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.server.MessageReference)
    */
   public void cancel(final MessageReference reference, final long timeBase) throws Exception
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
    */
   public void cancel(final Transaction tx, final MessageReference ref)
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#cancelRedistributor()
    */
   public void cancelRedistributor() throws Exception
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#changeReferencePriority(long, byte)
    */
   public boolean changeReferencePriority(final long messageID, final byte newPriority) throws Exception
   {
      // no-op
      return false;
   }

/* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#changeReferencesPriority(org.hornetq.core.filter.Filter, byte)
    */
   public int changeReferencesPriority(Filter filter, byte newPriority) throws Exception
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#checkDLQ(org.hornetq.core.server.MessageReference)
    */
   public boolean checkRedelivery(final MessageReference ref, final long timeBase) throws Exception
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#deleteAllReferences()
    */
   public int deleteAllReferences() throws Exception
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#deleteMatchingReferences(org.hornetq.core.filter.Filter)
    */
   public int deleteMatchingReferences(final Filter filter) throws Exception
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#deleteReference(long)
    */
   public boolean deleteReference(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#deliverAsync()
    */
   public void deliverAsync()
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#deliverNow()
    */
   public void deliverNow()
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#expire(org.hornetq.core.server.MessageReference)
    */
   public void expire(final MessageReference ref) throws Exception
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#expireReference(long)
    */
   public boolean expireReference(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#expireReferences()
    */
   public void expireReferences() throws Exception
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#expireReferences(org.hornetq.core.filter.Filter)
    */
   public int expireReferences(final Filter filter) throws Exception
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getConsumerCount()
    */
   public int getConsumerCount()
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getConsumers()
    */
   public Set<Consumer> getConsumers()
   {
      // no-op
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getDeliveringCount()
    */
   public int getDeliveringCount()
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getFilter()
    */
   public Filter getFilter()
   {
      // no-op
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getMessageCount()
    */
   public long getMessageCount()
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getMessagesAdded()
    */
   public long getMessagesAdded()
   {
      // no-op
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
      // no-op
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
      // no-op
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getScheduledCount()
    */
   public int getScheduledCount()
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getScheduledMessages()
    */
   public List<MessageReference> getScheduledMessages()
   {
      // no-op
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#isDurable()
    */
   public boolean isDurable()
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#isPaused()
    */
   public boolean isPaused()
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#isTemporary()
    */
   public boolean isTemporary()
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#iterator()
    */
   public LinkedListIterator<MessageReference> iterator()
   {
      // no-op
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#list(org.hornetq.core.filter.Filter)
    */
   public List<MessageReference> list(final Filter filter)
   {
      // no-op
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#lockDelivery()
    */
   public void lockDelivery()
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#moveReference(long, org.hornetq.utils.SimpleString)
    */
   public boolean moveReference(final long messageID, final SimpleString toAddress) throws Exception
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#moveReferences(org.hornetq.core.filter.Filter, org.hornetq.utils.SimpleString)
    */
   public int moveReferences(final Filter filter, final SimpleString toAddress) throws Exception
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#pause()
    */
   public void pause()
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#reacknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
    */
   public void reacknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#referenceHandled()
    */
   public void referenceHandled()
   {
      // no-op

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
      // no-op
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#removeReferenceWithID(long)
    */
   public MessageReference removeReferenceWithID(final long id) throws Exception
   {
      // no-op
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#resume()
    */
   public void resume()
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#sendMessageToDeadLetterAddress(long)
    */
   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#sendMessagesToDeadLetterAddress(org.hornetq.core.filter.Filter)
    */
   public int sendMessagesToDeadLetterAddress(Filter filter) throws Exception
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#setExpiryAddress(org.hornetq.utils.SimpleString)
    */
   public void setExpiryAddress(final SimpleString expiryAddress)
   {
      // no-op

   }

   // no-op

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#unlockDelivery()
    */
   public void unlockDelivery()
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Bindable#route(org.hornetq.core.server.ServerMessage, org.hornetq.core.server.RoutingContext)
    */
   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      // no-op

   }

   public boolean hasMatchingConsumer(final ServerMessage message)
   {
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#checkDLQ(org.hornetq.core.server.MessageReference, java.util.concurrent.Executor)
    */
   public boolean checkDLQ(final MessageReference ref, final Executor ioExecutor) throws Exception
   {
      // no-op
      return false;
   }

   public Executor getExecutor()
   {
      // no-op
      return null;
   }

   public void addLast(MessageReference ref, boolean direct)
   {
      // no-op

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
      // no-op
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#moveReferences(org.hornetq.core.filter.Filter, org.hornetq.api.core.SimpleString, boolean)
    */
   public int moveReferences(Filter filter, SimpleString toAddress, boolean rejectDuplicates) throws Exception
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#forceDelivery()
    */
   public void forceDelivery()
   {
      // no-op

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getInstantMessageCount()
    */
   public long getInstantMessageCount()
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getInstantMessagesAdded()
    */
   public long getInstantMessagesAdded()
   {
      // no-op
      return 0;
   }

}