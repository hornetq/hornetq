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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;


/**
 * 
 * A Queue
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface Queue
{     
   HandleStatus addLast(MessageReference ref);
   
   HandleStatus addFirst(MessageReference ref);
   
   /**
    * This method is used to add a List of MessageReferences atomically at the head of the list.
    * Useful when cancelling messages and guaranteeing ordering
    * @param list
    */
   void addListFirst(LinkedList<MessageReference> list);
         
   void deliverAsync(Executor executor);
   
   void addConsumer(Consumer consumer);

   boolean removeConsumer(Consumer consumer) throws Exception;
   
   int getConsumerCount();
   
   List<MessageReference> list(Filter filter);
   
   long getPersistenceID();
   
   void setPersistenceID(long id);
   
   Filter getFilter();
   
   int getMessageCount();
   
   int getDeliveringCount();
   
   void referenceAcknowledged(MessageReference ref) throws Exception;
  
   void referenceCancelled();

   void referenceHandled();
   
   int getScheduledCount();
   
   List<MessageReference> getScheduledMessages();

   int getSizeBytes();
   
   DistributionPolicy getDistributionPolicy();
   
   void setDistributionPolicy(DistributionPolicy policy); 
   
   boolean isClustered();
    
   boolean isDurable();
   
   boolean isTemporary();
   
   SimpleString getName();
   
   int getMessagesAdded();

   MessageReference removeReferenceWithID(long id) throws Exception;
   
   /** Remove message from queue, add it to the scheduled delivery list without affect reference counting */
   void rescheduleDelivery(long id, long scheduledDeliveryTime);
   
   MessageReference getReference(long id);
   
   int deleteAllReferences(StorageManager storageManager) throws Exception;

   boolean deleteReference(long messageID, StorageManager storageManager)
         throws Exception;

   int deleteMatchingReferences(Filter filter, StorageManager storageManager)
         throws Exception;

   boolean expireMessage(long messageID, StorageManager storageManager,
         PostOffice postOffice,
         HierarchicalRepository<QueueSettings> queueSettingsRepository)
         throws Exception;

   /**
    * Flagged all the messages in the queue which matches the filter as <em>expired</em>
    */
   int expireMessages(Filter filter,
                      StorageManager storageManager,
                      PostOffice postOffice,
                      HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception;

   void expireMessages(final StorageManager storageManager,
                                final PostOffice postOffice,
                                final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception;

   boolean sendMessageToDeadLetterAddress(long messageID, StorageManager storageManager,
         PostOffice postOffice,
         HierarchicalRepository<QueueSettings> queueSettingsRepository)
         throws Exception;

   boolean changeMessagePriority(long messageID, byte newPriority,
         StorageManager storageManager, PostOffice postOffice,
         HierarchicalRepository<QueueSettings> queueSettingsRepository)
         throws Exception;

   boolean moveMessage(long messageID, SimpleString toAddress,
         StorageManager storageManager, PostOffice postOffice) throws Exception;

   int moveMessages(Filter filter, SimpleString toAddress, StorageManager storageManager, PostOffice postOffice) throws Exception;

   void setBackup();
   
   boolean activate();
   
   void activateNow(Executor executor);
   
   boolean isBackup();
   
   MessageReference removeFirst();
   
   boolean consumerFailedOver();   
   
   //Only used in testing
   void deliverNow();

}
