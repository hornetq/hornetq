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

package org.jboss.messaging.core.postoffice.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.impl.PageTransactionInfoImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.AddressManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.SendLock;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.SendLockImpl;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionOperation;
import org.jboss.messaging.core.transaction.TransactionPropertyIndexes;
import org.jboss.messaging.core.transaction.Transaction.State;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.messaging.util.SimpleString;

/**
 * A PostOfficeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class PostOfficeImpl implements PostOffice
{
   private static final Logger log = Logger.getLogger(PostOfficeImpl.class);

   private static final List<MessageReference> emptyList = Collections.<MessageReference> emptyList();

   private final AddressManager addressManager;

   private final QueueFactory queueFactory;

   private final boolean checkAllowable;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private volatile boolean started;

   private volatile boolean backup;

   private final ManagementService managementService;

   private final Map<SimpleString, SendLock> addressLocks = new HashMap<SimpleString, SendLock>();

   private ScheduledThreadPoolExecutor messageExpiryExecutor;

   private final long messageExpiryScanPeriod;

   private final int messageExpiryThreadPriority;

   private final ConcurrentMap<SimpleString, DuplicateIDCache> duplicateIDCaches = new ConcurrentHashMap<SimpleString, DuplicateIDCache>();

   private final int idCacheSize;

   private final boolean persistIDCache;

   public PostOfficeImpl(final StorageManager storageManager,
                         final PagingManager pagingManager,
                         final QueueFactory bindableFactory,
                         final ManagementService managementService,
                         final long messageExpiryScanPeriod,
                         final int messageExpiryThreadPriority,
                         final boolean checkAllowable,
                         final boolean enableWildCardRouting,
                         final boolean backup,
                         final int idCacheSize,
                         final boolean persistIDCache)
   {
      this.storageManager = storageManager;

      this.queueFactory = bindableFactory;

      this.managementService = managementService;

      this.checkAllowable = checkAllowable;

      this.pagingManager = pagingManager;

      this.messageExpiryScanPeriod = messageExpiryScanPeriod;

      this.messageExpiryThreadPriority = messageExpiryThreadPriority;

      if (enableWildCardRouting)
      {
         addressManager = new WildcardAddressManager();
      }
      else
      {
         addressManager = new SimpleAddressManager();
      }

      this.backup = backup;

      this.idCacheSize = idCacheSize;

      this.persistIDCache = persistIDCache;
   }

   // MessagingComponent implementation ---------------------------------------

   public void start() throws Exception
   {
      if (pagingManager != null)
      {
         pagingManager.setPostOffice(this);
      }

      // Injecting the postoffice (itself) on queueFactory for paging-control
      queueFactory.setPostOffice(this);

      if (messageExpiryScanPeriod > 0)
      {
         MessageExpiryRunner messageExpiryRunner = new MessageExpiryRunner();

         messageExpiryExecutor = new ScheduledThreadPoolExecutor(1, new JBMThreadFactory("JBM-scheduled-threads",
                                                                                         messageExpiryThreadPriority));
         messageExpiryExecutor.scheduleAtFixedRate(messageExpiryRunner,
                                                   messageExpiryScanPeriod,
                                                   messageExpiryScanPeriod,
                                                   TimeUnit.MILLISECONDS);
      }
      started = true;
   }

   public void stop() throws Exception
   {
      if (messageExpiryExecutor != null)
      {
         messageExpiryExecutor.shutdown();
      }


      addressManager.clear();

      // Release all the locks
      for (SendLock lock : addressLocks.values())
      {
         lock.close();
      }

      addressLocks.clear();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   // PostOffice implementation -----------------------------------------------

   public synchronized boolean addDestination(final SimpleString address, final boolean durable) throws Exception
   {
      boolean added = addressManager.addDestination(address);

      if (added)
      {
         if (durable)
         {
            storageManager.addDestination(address);
         }

         managementService.registerAddress(address);
      }

      return added;
   }

   public synchronized boolean removeDestination(final SimpleString address, final boolean durable) throws Exception
   {
      boolean removed = addressManager.removeDestination(address);

      if (removed)
      {
         if (durable)
         {
            storageManager.deleteDestination(address);
         }
         managementService.unregisterAddress(address);
      }

      addressLocks.remove(address);

      return removed;
   }

   public synchronized boolean containsDestination(final SimpleString address)
   {
      return addressManager.containsDestination(address);
   }

   public Set<SimpleString> listAllDestinations()
   {
      return addressManager.getDestinations();
   }

   // TODO - needs to be synchronized to prevent happening concurrently with activate().
   // (and possible removeBinding and other methods)
   // Otherwise can have situation where createQueue comes in before failover, then failover occurs
   // and post office is activated but queue remains unactivated after failover so delivery never occurs
   // even though failover is complete
   public synchronized void addBinding(final Binding binding) throws Exception
   {
      addBindingInMemory(binding);
   }

   public synchronized Binding removeBinding(final SimpleString uniqueName) throws Exception
   {
      Binding binding = removeBindingInMemory(uniqueName);

      if (binding.isQueueBinding())
      {
         managementService.unregisterQueue(uniqueName, binding.getAddress());
      }

      return binding;
   }

   public Bindings getBindingsForAddress(final SimpleString address)
   {
      Bindings bindings = addressManager.getBindings(address);

      if (bindings == null)
      {
         bindings = new BindingsImpl();
      }

      return bindings;
   }

   public Binding getBinding(final SimpleString name)
   {
      return addressManager.getBinding(name);
   }

   public void route(final ServerMessage message, Transaction tx) throws Exception
   {
      SimpleString address = message.getDestination();

      if (checkAllowable)
      {
         if (!addressManager.containsDestination(address))
         {
            throw new MessagingException(MessagingException.ADDRESS_DOES_NOT_EXIST,
                                         "Cannot route to address " + address);
         }
      }

      SimpleString duplicateID = (SimpleString)message.getProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID);

      DuplicateIDCache cache = null;

      if (duplicateID != null)
      {
         cache = getDuplicateIDCache(message.getDestination());

         if (cache.contains(duplicateID))
         {
            if (tx == null)
            {
               log.warn("Duplicate message detected - message will not be routed");
            }
            else
            {
               log.warn("Duplicate message detected - transaction will be rejected");

               tx.markAsRollbackOnly(null);
            }

            return;
         }
      }

      boolean startedTx = false;

      if (cache != null)
      {
         cache.addToCache(duplicateID, tx);

         if (tx == null)
         {
            // We need to store the duplicate id atomically with the message storage, so we need to create a tx for this

            tx = new TransactionImpl(storageManager);

            startedTx = true;
         }
      }
      
      if (tx == null)
      {
         if (pagingManager.page(message, true))
         {
            return;
         }
      }
      else
      {
         SimpleString destination = message.getDestination();
         
         boolean depage = tx.getProperty(TransactionPropertyIndexes.IS_DEPAGE) != null;
         
         if (!depage && pagingManager.isPaging(destination))
         {
            getPageOperation(tx).addMessageToPage(message);
            
            return;
         }
      }

      Bindings bindings = addressManager.getBindings(address);

      if (bindings != null)
      {
         bindings.route(message, tx);
      }

      if (startedTx)
      {
         tx.commit();
      }
   }

   public void route(final ServerMessage message) throws Exception
   {
      route(message, null);
   }

   public PagingManager getPagingManager()
   {
      return pagingManager;
   }

   public List<Queue> activate()
   {
      backup = false;

      Map<SimpleString, Binding> nameMap = addressManager.getBindings();

      List<Queue> queues = new ArrayList<Queue>();

      for (Binding binding : nameMap.values())
      {
         if (binding.isQueueBinding())
         {
            Queue queue = (Queue)binding.getBindable();

            boolean activated = queue.activate();

            if (!activated)
            {
               queues.add(queue);
            }
         }
      }

      return queues;
   }

   public synchronized SendLock getAddressLock(final SimpleString address)
   {
      SendLock lock = addressLocks.get(address);

      if (lock == null)
      {
         lock = new SendLockImpl();

         addressLocks.put(address, lock);
      }

      return lock;
   }

   public DuplicateIDCache getDuplicateIDCache(final SimpleString address)
   {
      DuplicateIDCache cache = duplicateIDCaches.get(address);

      if (cache == null)
      {
         cache = new DuplicateIDCacheImpl(address, idCacheSize, storageManager, persistIDCache);

         DuplicateIDCache oldCache = duplicateIDCaches.putIfAbsent(address, cache);

         if (oldCache != null)
         {
            cache = oldCache;
         }
      }

      return cache;
   }

   // Private -----------------------------------------------------------------
   
   private final PageMessageOperation getPageOperation(final Transaction tx)
   {
      PageMessageOperation oper = (PageMessageOperation)tx.getProperty(TransactionPropertyIndexes.PAGE_MESSAGES_OPERATION);
      
      if (oper == null)
      {         
         oper = new PageMessageOperation();
         
         tx.putProperty(TransactionPropertyIndexes.PAGE_MESSAGES_OPERATION, oper);
         
         tx.addOperation(oper);
      }
      
      return oper;
   }

   private void addBindingInMemory(final Binding binding) throws Exception
   {
      boolean exists = addressManager.addMapping(binding.getAddress(), binding);

      if (!exists)
      {
         managementService.registerAddress(binding.getAddress());
      }

      if (binding.isQueueBinding())
      {
         Queue queue = (Queue)binding.getBindable();

         if (backup)
         {
            queue.setBackup();
         }

         managementService.registerQueue(queue, binding.getAddress(), storageManager);
      }

      addressManager.addBinding(binding);
   }

   private Binding removeBindingInMemory(final SimpleString bindingName) throws Exception
   {
      Binding binding = addressManager.removeBinding(bindingName);

      if (addressManager.removeMapping(binding.getAddress(), bindingName))
      {
         managementService.unregisterAddress(binding.getAddress());
      }

      return binding;
   }

   private class MessageExpiryRunner implements Runnable
   {
      public void run()
      {
         Map<SimpleString, Binding> nameMap = addressManager.getBindings();

         List<Queue> queues = new ArrayList<Queue>();

         for (Binding binding : nameMap.values())
         {
            if (binding.isQueueBinding())
            {
               Queue queue = (Queue)binding.getBindable();

               queues.add(queue);
            }
         }

         for (Queue queue : queues)
         {
            try
            {
               queue.expireMessages();
            }
            catch (Exception e)
            {
               log.error("failed to expire messages for queue " + queue.getName(), e);
            }
         }
      }
   }

   private class PageMessageOperation implements TransactionOperation
   {
      private final List<ServerMessage> messagesToPage = new ArrayList<ServerMessage>();
      
      void addMessageToPage(final ServerMessage message)
      {
         messagesToPage.add(message);
      }

      public void afterCommit(final Transaction tx) throws Exception
      {
         // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the
         // transaction until all the messages were added to the queue
         // or else we could deliver the messages out of order
         
         PageTransactionInfo pageTransaction = (PageTransactionInfo)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);
         
         if (pageTransaction != null)
         {
            pageTransaction.commit();
         }
      }

      public void afterPrepare(final Transaction tx) throws Exception
      {
      }

      public void afterRollback(final Transaction tx) throws Exception
      {
         PageTransactionInfo pageTransaction = (PageTransactionInfo)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

         if (tx.getState() == State.PREPARED && pageTransaction != null)
         {
            pageTransaction.rollback();
         }
      }

      public void beforeCommit(final Transaction tx) throws Exception
      {
         if (tx.getState() != Transaction.State.PREPARED)
         {
            pageMessages(tx);
         }                
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
         pageMessages(tx);
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
      }

      private void pageMessages(final Transaction tx) throws Exception
      {
         if (!messagesToPage.isEmpty())
         {
            PageTransactionInfo pageTransaction = (PageTransactionInfo)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

            if (pageTransaction == null)
            {
               pageTransaction = new PageTransactionInfoImpl(tx.getID());

               tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION, pageTransaction);

               // To avoid a race condition where depage happens before the transaction is completed, we need to inform
               // the pager about this transaction is being processed
               pagingManager.addTransaction(pageTransaction);
            }

            boolean pagingPersistent = false;

            HashSet<SimpleString> pagedDestinationsToSync = new HashSet<SimpleString>();

            // We only need to add the dupl id header once per transaction
            boolean first = true;
            for (ServerMessage message : messagesToPage)
            {
               // http://wiki.jboss.org/wiki/JBossMessaging2Paging
               // Explained under Transaction On Paging. (This is the item B)
               if (pagingManager.page(message, tx.getID(), first))
               {
                  if (message.isDurable())
                  {
                     // We only create pageTransactions if using persistent messages
                     pageTransaction.increment();
                     pagingPersistent = true;
                     pagedDestinationsToSync.add(message.getDestination());
                  }
               }
               else
               {
                  // This could happen when the PageStore left the pageState

                  // TODO is this correct - don't we lose transactionality here???
                  route(message, null);
               }
               first = false;
            }

            if (pagingPersistent)
            {
               tx.putProperty(TransactionPropertyIndexes.CONTAINS_PERSISTENT, true);

               if (!pagedDestinationsToSync.isEmpty())
               {
                  pagingManager.sync(pagedDestinationsToSync);
                  storageManager.storePageTransaction(tx.getID(), pageTransaction);
               }
            }
         }
      }

   }

}
