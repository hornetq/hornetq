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
import java.util.List;
import java.util.concurrent.Executor;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.LinkedListIterator;

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
   
   PageSubscription getPageSubscription();

   boolean isDurable();

   boolean isTemporary();

   void addConsumer(Consumer consumer) throws Exception;

   void removeConsumer(Consumer consumer) throws Exception;

   int getConsumerCount();
   
   void reload(MessageReference ref);
   
   void addTail(MessageReference ref);

   void addTail(MessageReference ref, boolean direct);

   void addHead(MessageReference ref);
   
   void addHead(final List<MessageReference> refs);

   void acknowledge(MessageReference ref) throws Exception;

   void acknowledge(Transaction tx, MessageReference ref) throws Exception;

   void reacknowledge(Transaction tx, MessageReference ref) throws Exception;

   void cancel(Transaction tx, MessageReference ref);

   void cancel(MessageReference reference, long timeBase) throws Exception;

   void deliverAsync();
   
   /** This method will make sure that any pending message (including paged message) will be delivered  */
   void forceDelivery();

   void destroyPaging() throws Exception;

   long getMessageCount();
   
   /** Return the current message count without waiting for scheduled executors to finish */
   long getInstantMessageCount();

   int getDeliveringCount();

   void referenceHandled();

   int getScheduledCount();

   List<MessageReference> getScheduledMessages();

   long getMessagesAdded();
   
   long getInstantMessagesAdded();

   MessageReference removeReferenceWithID(long id) throws Exception;

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

   int sendMessagesToDeadLetterAddress(Filter filter) throws Exception;

   boolean changeReferencePriority(long messageID, byte newPriority) throws Exception;

   int changeReferencesPriority(Filter filter, byte newPriority) throws Exception;

   boolean moveReference(long messageID, SimpleString toAddress) throws Exception;

   boolean moveReference(long messageID, SimpleString toAddress, boolean rejectDuplicates) throws Exception;

   int moveReferences(Filter filter, SimpleString toAddress) throws Exception;

   int moveReferences(Filter filter, SimpleString toAddress, boolean rejectDuplicates) throws Exception;

   void addRedistributor(long delay);

   void cancelRedistributor() throws Exception;

   boolean hasMatchingConsumer(ServerMessage message);

   Collection<Consumer> getConsumers();

   boolean checkRedelivery(MessageReference ref, long timeBase) throws Exception;

   LinkedListIterator<MessageReference> iterator();

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
   
   Executor getExecutor();
   
   void resetAllIterators();

   boolean flushExecutor();
   
   void close() throws Exception;
   
   boolean isDirectDeliver();

   SimpleString getAddress();
   
   /**
    * We can't send stuff to DLQ on queues used on clustered-bridge-communication
    * @return
    */
   boolean isInternalQueue();
   
   void setInternalQueue(boolean internalQueue);
}
