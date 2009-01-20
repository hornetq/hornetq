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

package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.message.impl.MessageImpl.HDR_ORIGINAL_DESTINATION;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Divert;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.util.SimpleString;

/**
 * A DivertImpl simply diverts a message to a different forwardAddress
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 19 Dec 2008 10:57:49
 *
 *
 */
public class DivertImpl implements Divert
{
   private static final Logger log = Logger.getLogger(DivertImpl.class);

   private final PostOffice postOffice;

   private final SimpleString forwardAddress;

   private final SimpleString uniqueName;

   private final SimpleString routingName;

   private final boolean exclusive;

   private final Filter filter;

   private final Transformer transformer;
   
   private final PagingManager pagingManager;
   
   private final StorageManager storageManager;

   public DivertImpl(final SimpleString forwardAddress,
                     final SimpleString uniqueName,
                     final SimpleString routingName,
                     final boolean exclusive,
                     final Filter filter,
                     final Transformer transformer,
                     final PostOffice postOffice,
                     final PagingManager pagingManager,
                     final StorageManager storageManager)
   {
      this.forwardAddress = forwardAddress;

      this.uniqueName = uniqueName;

      this.routingName = routingName;

      this.exclusive = exclusive;

      this.filter = filter;

      this.transformer = transformer;

      this.postOffice = postOffice;
      
      this.pagingManager = pagingManager;
      
      this.storageManager = storageManager;
   }

   public boolean accept(final ServerMessage message) throws Exception
   {
      if (filter != null && !filter.match(message))
      {
         return false;
      }
      else
      {
         //We need to increment ref count here to ensure that the message doesn't get stored, deleted and stored again in a single route which
         //can occur if the message is routed to a queue, then acked before it's routed here
         
         //TODO - combine with similar code in QueueImpl.accept()
         
         int count = message.incrementRefCount();
         
         if (count == 1)
         {
            PagingStore store = pagingManager.getPageStore(message.getDestination());
            
            store.addSize(message.getMemoryEstimate());
         }
       
         if (message.isDurable())
         {
            message.incrementDurableRefCount();
         }
         
         return true;
      }
   }

   public void route(ServerMessage message, final Transaction tx) throws Exception
   {      
      SimpleString originalDestination = message.getDestination();

      message.setDestination(forwardAddress);

      message.putStringProperty(HDR_ORIGINAL_DESTINATION, originalDestination);

      if (transformer != null)
      {
         message = transformer.transform(message);
      }

      postOffice.route(message, tx);
      
      //Decrement the ref count here - and delete the message if necessary
      
      //TODO combine this with code in QueueImpl::postAcknowledge
      
      if (message.isDurable())
      {
         int count = message.decrementDurableRefCount();

         if (count == 0)
         {
            storageManager.deleteMessage(message.getMessageID());
         }
      }

      // TODO: We could optimize this by storing the paging-store for the address on the Queue. We would need to know
      // the Address for the Queue
      PagingStore store = pagingManager.getPageStore(message.getDestination());

      if (message.decrementRefCount() == 0)
      {
         store.addSize(-message.getMemoryEstimate());         
      }
   }

   public SimpleString getRoutingName()
   {
      return routingName;
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
   }

   public boolean isExclusive()
   {
      return exclusive;
   }
}
