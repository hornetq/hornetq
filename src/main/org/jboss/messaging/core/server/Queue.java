/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.utils.SimpleString;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

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
   MessageReference reroute(ServerMessage message, Transaction tx) throws Exception;

   SimpleString getName();

   long getPersistenceID();

   void setPersistenceID(long id);

   Filter getFilter();

   boolean isDurable();

   boolean isTemporary();

   void addConsumer(Consumer consumer) throws Exception;

   boolean removeConsumer(Consumer consumer) throws Exception;

   int getConsumerCount();

   Set<Consumer> getConsumers();

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

   Distributor getDistributionPolicy();

   void setDistributionPolicy(Distributor policy);

   int getMessagesAdded();

   MessageReference removeReferenceWithID(long id) throws Exception;
   
   MessageReference removeFirstReference(long id) throws Exception;

   MessageReference getReference(long id);

   int deleteAllReferences() throws Exception;

   boolean deleteReference(long messageID) throws Exception;

   int deleteMatchingReferences(Filter filter) throws Exception;

   boolean expireMessage(long messageID) throws Exception;

   /**
    * Expire all the messages in the queue which matches the filter
    */
   int expireMessages(Filter filter) throws Exception;

   void expireMessages() throws Exception;

   void expire(MessageReference ref) throws Exception;

   boolean sendMessageToDeadLetterAddress(long messageID) throws Exception;

   boolean changeMessagePriority(long messageID, byte newPriority) throws Exception;

   boolean moveMessage(long messageID, SimpleString toAddress) throws Exception;

   int moveMessages(Filter filter, SimpleString toAddress) throws Exception;

   void setBackup();

   boolean activate();

   void activateNow(Executor executor);

   boolean isBackup();

   boolean consumerFailedOver();

   void addRedistributor(long delay, Executor executor);

   void cancelRedistributor() throws Exception;

   // Only used in testing
   void deliverNow();

   boolean checkDLQ(MessageReference ref) throws Exception;
}
