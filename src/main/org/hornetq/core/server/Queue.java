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

package org.hornetq.core.server;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A Queue
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface Queue extends Bindable
{
   SimpleString getName();

   long getID();

   Filter getFilter();

   boolean isDurable();

   boolean isTemporary();

   void addConsumer(Consumer consumer) throws Exception;

   boolean removeConsumer(Consumer consumer) throws Exception;

   int getConsumerCount();

   void addLast(MessageReference ref);

   void addFirst(MessageReference ref);

   void acknowledge(MessageReference ref) throws Exception;

   void acknowledge(Transaction tx, MessageReference ref) throws Exception;

   void reacknowledge(Transaction tx, MessageReference ref) throws Exception;

   void cancel(Transaction tx, MessageReference ref) throws Exception;

   void cancel(MessageReference reference) throws Exception;

   void deliverAsync(Executor executor);

   List<MessageReference> list(Filter filter);

   int getMessageCount();

   int getDeliveringCount();

   void referenceHandled();

   int getScheduledCount();

   List<MessageReference> getScheduledMessages();

//   Distributor getDistributionPolicy();
//
//   void setDistributionPolicy(Distributor policy);

   int getMessagesAdded();

   MessageReference removeReferenceWithID(long id) throws Exception;
   
   MessageReference removeFirstReference(long id) throws Exception;

   MessageReference getReference(long id);

   int deleteAllReferences() throws Exception;

   boolean deleteReference(long messageID) throws Exception;

   int deleteMatchingReferences(Filter filter) throws Exception;

   boolean expireReference(long messageID) throws Exception;

   /**
    * Expire all the references in the queue which matches the filter
    */
   int expireReferences(Filter filter) throws Exception;

   void expireReferences() throws Exception;

   void expire(MessageReference ref) throws Exception;

   boolean sendMessageToDeadLetterAddress(long messageID) throws Exception;

   boolean changeReferencePriority(long messageID, byte newPriority) throws Exception;

   boolean moveReference(long messageID, SimpleString toAddress) throws Exception;

   int moveReferences(Filter filter, SimpleString toAddress) throws Exception;

   void addRedistributor(long delay, Executor executor);

   void cancelRedistributor() throws Exception;
   
   boolean hasMatchingConsumer(ServerMessage message);

   // Only used in testing
   void deliverNow();
   
   Collection<Consumer> getConsumers();
   
   boolean checkDLQ(MessageReference ref) throws Exception;
   
   void lockDelivery();
   
   void unlockDelivery();

   /**
    * @return an immutable iterator which does not allow to remove references
    */
   Iterator<MessageReference> iterator();
   
   void setExpiryAddress(SimpleString expiryAddress);
   /**
    * Pauses the queue. It will receive messages but won't give them to the consumers until resumed.
    * If a queue is paused, pausing it again will only throw a warning. 
    * To check if a queue is paused, invoke <i>isPaused()</i>
    */
   void pause();
   /**
    * Resumes the delivery of message for the queue. 
    * If a queue is resumed, resuming it again will only throw a warning. 
    * To check if a queue is resumed, invoke <i>isPaused()</i>
    */
   void resume();
   /**
    * 
    * @return true if paused, false otherwise.
    */
   boolean isPaused();

}
