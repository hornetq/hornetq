/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.postoffice.FlowController;
import org.jboss.messaging.core.persistence.PersistenceManager;


/**
 * 
 * A Queue
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 *
 */
public interface Queue
{
   public static final int NUM_PRIORITIES = 10;
      
   HandleStatus addLast(MessageReference ref);
   
   HandleStatus addFirst(MessageReference ref);
   
   /**
    * This method is used to add a List of MessageReferences atomically at the head of the list.
    * Useful when cancelling messages and guaranteeing ordering
    * @param list
    */
   void addListFirst(LinkedList<MessageReference> list);
   
   void deliver();
   
   void addConsumer(Consumer consumer);

   boolean removeConsumer(Consumer consumer);
   
   int getConsumerCount();
   
   List<MessageReference> list(Filter filter);
   
   void removeAllReferences();

   void removeReference(MessageReference messageReference);

   void changePriority(final MessageReference messageReference, int priority);

   long getPersistenceID();
   
   void setPersistenceID(long id);
   
   Filter getFilter();
   
   void setFilter(Filter filter);
   
   int getMessageCount();
   
   int getDeliveringCount();
   
   void referenceAcknowledged() throws Exception;
  
   void referenceCancelled();
   
   int getScheduledCount();
          
   int getMaxSize();
   
   void setMaxSize(int maxSize);
   
   DistributionPolicy getDistributionPolicy();
   
   void setDistributionPolicy(DistributionPolicy policy); 
   
   boolean isClustered();
   
   boolean isTemporary();
   
   boolean isDurable();
   
   String getName();
   
   int getMessagesAdded();

   FlowController getFlowController();
   
   void setFlowController(FlowController flowController);

   void move(MessageReference messageReference, Queue queue, PersistenceManager persistenceManager) throws Exception;
}
