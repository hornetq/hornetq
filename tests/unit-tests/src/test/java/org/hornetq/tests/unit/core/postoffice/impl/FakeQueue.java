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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.LinkedListIterator;
import org.hornetq.utils.ReferenceCounter;

/**
 * A FakeQueue
 *
 * @author tim
 *
 *
 */
public class FakeQueue implements Queue
{

   @Override
   public boolean isInternalQueue()
   {
      // no-op
      return false;
   }

   @Override
   public void setConsumersRefCount(HornetQServer server)
   {

   }

   @Override
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

   @Override
   public void acknowledge(final MessageReference ref) throws Exception
   {
      // no-op

   }

   @Override
   public void acknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      // no-op

   }

   @Override
   public void addConsumer(final Consumer consumer) throws Exception
   {
      // no-op

   }

   @Override
   public void addRedistributor(final long delay)
   {
      // no-op

   }

   @Override
   public void cancel(final MessageReference reference, final long timeBase) throws Exception
   {
      // no-op

   }

   @Override
   public void cancel(final Transaction tx, final MessageReference ref)
   {
      // no-op

   }

   @Override
   public void cancelRedistributor() throws Exception
   {
      // no-op

   }

   @Override
   public boolean changeReferencePriority(final long messageID, final byte newPriority) throws Exception
   {
      // no-op
      return false;
   }

@Override
   public int changeReferencesPriority(Filter filter, byte newPriority) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public boolean checkRedelivery(final MessageReference ref, final long timeBase) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public int deleteAllReferences() throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public int deleteMatchingReferences(final Filter filter) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public boolean deleteReference(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public void deliverAsync()
   {
      // no-op

   }

   @Override
   public void expire(final MessageReference ref) throws Exception
   {
      // no-op

   }

   @Override
   public boolean expireReference(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public void expireReferences() throws Exception
   {
      // no-op

   }

   @Override
   public int expireReferences(final Filter filter) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public int getConsumerCount()
   {
      // no-op
      return 0;
   }

   @Override
   public ReferenceCounter getConsumersRefCount()
   {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   @Override
   public Set<Consumer> getConsumers()
   {
      // no-op
      return null;
   }

   @Override
   public int getDeliveringCount()
   {
      // no-op
      return 0;
   }

   @Override
   public Filter getFilter()
   {
      // no-op
      return null;
   }

   @Override
   public long getMessageCount()
   {
      // no-op
      return 0;
   }

   @Override
   public long getMessageCount(long timeout)
   {
      return 0;
   }

   @Override
   public long getMessagesAdded()
   {
      // no-op
      return 0;
   }
   
   @Override
   public void resetMessagesAdded()
   {
      // no-op
      
   }

   @Override
   public long getMessagesAdded(long timeout)
   {
      return 0;
   }

   @Override
   public SimpleString getName()
   {
      return name;
   }

   public SimpleString getAddress()
   {
      // no-op
      return null;
   }

   @Override
   public long getID()
   {
      return id;
   }

   @Override
   public MessageReference getReference(final long id1)
   {
      // no-op
      return null;
   }

   @Override
   public int getScheduledCount()
   {
      // no-op
      return 0;
   }

   @Override
   public List<MessageReference> getScheduledMessages()
   {
      // no-op
      return null;
   }

   @Override
   public boolean isDurable()
   {
      // no-op
      return false;
   }

   @Override
   public boolean isPaused()
   {
      // no-op
      return false;
   }

   @Override
   public boolean isTemporary()
   {
      // no-op
      return false;
   }

   @Override
   public LinkedListIterator<MessageReference> iterator()
   {
      // no-op
      return null;
   }

   @Override
   public boolean moveReference(final long messageID, final SimpleString toAddress) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public int moveReferences(final Filter filter, final SimpleString toAddress) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public void pause()
   {
      // no-op

   }

   @Override
   public void reacknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      // no-op

   }

   public void referenceHandled()
   {
      // no-op

   }

   public void removeConsumer(final Consumer consumer)
   {
   }

   public MessageReference removeFirstReference(final long id1) throws Exception
   {
      // no-op
      return null;
   }

   public MessageReference removeReferenceWithID(final long id1) throws Exception
   {
      // no-op
      return null;
   }

   public void resume()
   {
      // no-op

   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      // no-op
      return false;
   }

   public int sendMessagesToDeadLetterAddress(Filter filter) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public void setExpiryAddress(final SimpleString expiryAddress)
   {
      // no-op

   }

   @Override
   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      // no-op

   }

   public boolean hasMatchingConsumer(final ServerMessage message)
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

   @Override
   public PageSubscription getPageSubscription()
   {
      return subs;
   }

   public void setPageSubscription(PageSubscription sub)
   {
      this.subs = sub;
   }

   @Override
   public boolean moveReference(long messageID, SimpleString toAddress, boolean rejectDuplicates) throws Exception
   {
      // no-op
      return false;
   }

   @Override
   public int moveReferences(Filter filter, SimpleString toAddress, boolean rejectDuplicates) throws Exception
   {
      // no-op
      return 0;
   }

   @Override
   public void forceDelivery()
   {
      // no-op

   }

   @Override
   public void deleteQueue() throws Exception
   {
      // no-op
   }

   @Override
   public long getInstantMessageCount()
   {
      // no-op
      return 0;
   }

   @Override
   public long getInstantMessagesAdded()
   {
      // no-op
      return 0;
   }

   /* (non-Javadoc)
   * @see org.hornetq.core.server.Queue#destroyPaging()
   */
   public void destroyPaging()
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getDeliveringMessages()
    */
   @Override
   public Map<String, List<MessageReference>> getDeliveringMessages()
   {
      return null;
   }

   @Override
   public LinkedListIterator<MessageReference> totalIterator()
   {
      // TODO Auto-generated method stub
      return null;
   }



}