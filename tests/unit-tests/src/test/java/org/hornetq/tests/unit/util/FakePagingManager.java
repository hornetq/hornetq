package org.hornetq.tests.unit.util;

import java.util.Collection;
import java.util.Map;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.ServerMessage;

public final class FakePagingManager implements PagingManager
{

   public void activate()
   {
   }

   public long addSize(final long size)
   {
      return 0;
   }

   public void addTransaction(final PageTransactionInfo pageTransaction)
   {
   }

   public PagingStore createPageStore(final SimpleString destination) throws Exception
   {
      return null;
   }

   public long getTotalMemory()
   {
      return 0;
   }

   public SimpleString[] getStoreNames()
   {
      return null;
   }

   public long getMaxMemory()
   {
      return 0;
   }

   public PagingStore getPageStore(final SimpleString address) throws Exception
   {
      return null;
   }

   public void deletePageStore(SimpleString storeName) throws Exception
   {
   }

   public PageTransactionInfo getTransaction(final long transactionID)
   {
      return null;
   }

   public boolean isBackup()
   {
      return false;
   }

   public boolean isGlobalPageMode()
   {
      return false;
   }

   public boolean isPaging(final SimpleString destination) throws Exception
   {
      return false;
   }

   public boolean page(final ServerMessage message, final boolean duplicateDetection) throws Exception
   {
      return false;
   }

   public boolean page(final ServerMessage message, final long transactionId, final boolean duplicateDetection)
            throws Exception
   {
      return false;
   }

   public void reloadStores() throws Exception
   {
   }

   public void removeTransaction(final long transactionID)
   {

   }

   public void setGlobalPageMode(final boolean globalMode)
   {
   }

   public void setPostOffice(final PostOffice postOffice)
   {
   }

   public void resumeDepages()
   {
   }

   public void sync(final Collection<SimpleString> destinationsToSync) throws Exception
   {
   }

   public boolean isStarted()
   {
      return false;
   }

   public void start() throws Exception
   {
   }

   public void stop() throws Exception
   {
   }

   /*
    * (non-Javadoc)
    * @see org.hornetq.core.paging.PagingManager#isGlobalFull()
    */
   public boolean isGlobalFull()
   {
      return false;
   }

   /*
    * (non-Javadoc)
    * @see org.hornetq.core.paging.PagingManager#getTransactions()
    */
   public Map<Long, PageTransactionInfo> getTransactions()
   {
      return null;
   }

   /*
    * (non-Javadoc)
    * @see org.hornetq.core.paging.PagingManager#processReload()
    */
   public void processReload()
   {
   }

   @Override
   public void disableCleanup()
   {
   }

   @Override
   public void resumeCleanup()
   {
   }

   /*
    * (non-Javadoc)
    * @see org.hornetq.core.settings.HierarchicalRepositoryChangeListener#onChange()
    */
   public void onChange()
   {
   }

   @Override
   public void lock()
   {
      // no-op
   }

   @Override
   public void unlock()
   {
      // no-op
   }

}
