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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.SimpleString;

public class FakeQueue implements Queue
{
   private SimpleString name;

   public FakeQueue(SimpleString name)
   {
      this.name = name;
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
    * @see org.hornetq.core.server.Queue#addConsumer(org.hornetq.core.server.Consumer)
    */
   public void addConsumer(Consumer consumer) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addFirst(org.hornetq.core.server.MessageReference)
    */
   public void addFirst(MessageReference ref)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addLast(org.hornetq.core.server.MessageReference)
    */
   public void addLast(MessageReference ref)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#addRedistributor(long, java.util.concurrent.Executor)
    */
   public void addRedistributor(long delay, Executor executor)
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.server.MessageReference)
    */
   public void cancel(MessageReference reference) throws Exception
   {
      // TODO Auto-generated method stub

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
    */
   public void cancel(Transaction tx, MessageReference ref) throws Exception
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
   public boolean changeReferencePriority(long messageID, byte newPriority) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#checkDLQ(org.hornetq.core.server.MessageReference)
    */
   public boolean checkDLQ(MessageReference ref) throws Exception
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
   public int deleteMatchingReferences(Filter filter) throws Exception
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
    * @see org.hornetq.core.server.Queue#deliverAsync(java.util.concurrent.Executor)
    */
   public void deliverAsync(Executor executor)
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
   public void expire(MessageReference ref) throws Exception
   {
      // TODO Auto-generated method stub

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
    * @see org.hornetq.core.server.Queue#expireReferences()
    */
   public void expireReferences() throws Exception
   {
      // TODO Auto-generated method stub

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
   public int getMessageCount()
   {
      // TODO Auto-generated method stub
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getMessagesAdded()
    */
   public int getMessagesAdded()
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

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#getID()
    */
   public long getID()
   {
      // TODO Auto-generated method stub
      return 0;
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
   public Iterator<MessageReference> iterator()
   {
      // TODO Auto-generated method stub
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#list(org.hornetq.core.filter.Filter)
    */
   public List<MessageReference> list(Filter filter)
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
   public boolean moveReference(long messageID, SimpleString toAddress) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#moveReferences(org.hornetq.core.filter.Filter, org.hornetq.utils.SimpleString)
    */
   public int moveReferences(Filter filter, SimpleString toAddress) throws Exception
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
   public void reacknowledge(Transaction tx, MessageReference ref) throws Exception
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
   public boolean removeConsumer(Consumer consumer) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#removeFirstReference(long)
    */
   public MessageReference removeFirstReference(long id) throws Exception
   {
      // TODO Auto-generated method stub
      return null;
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
    * @see org.hornetq.core.server.Queue#resume()
    */
   public void resume()
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
    * @see org.hornetq.core.server.Queue#setExpiryAddress(org.hornetq.utils.SimpleString)
    */
   public void setExpiryAddress(SimpleString expiryAddress)
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
   public void route(ServerMessage message, RoutingContext context) throws Exception
   {
      // TODO Auto-generated method stub

   }

   public boolean hasMatchingConsumer(ServerMessage message)
   {
      // TODO Auto-generated method stub
      return false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.Queue#checkDLQ(org.hornetq.core.server.MessageReference, java.util.concurrent.Executor)
    */
   public boolean checkDLQ(MessageReference ref, Executor ioExecutor) throws Exception
   {
      // TODO Auto-generated method stub
      return false;
   }

}