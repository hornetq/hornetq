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
package org.jboss.messaging.core.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Message;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.MessageStore;
import org.jboss.messaging.core.contract.PersistenceManager;
import org.jboss.messaging.core.contract.PersistenceManager.InitialLoadInfo;
import org.jboss.messaging.core.contract.PersistenceManager.ReferenceInfo;

/**
 * This channel implementation automatically pages message references to and from storage to prevent
 * more than a maximum number of references being stored in memory at once.
 * 
 * This allows us to support logical channels holding many millions of messages without running out
 * of memory.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class PagingChannelSupport extends ChannelSupport
{
   private static final Logger log = Logger.getLogger(PagingChannelSupport.class);
   
   private boolean trace = log.isTraceEnabled();
   
   protected List downCache;
   
   /**
    * The maximum number of references this channel will hold before going into paging mode
    */
   protected int fullSize = 200000;

   /**
    * The maximum number of references to load from storage in one go when unpaging
    */
   protected int pageSize = 2000;

   /**
    * The maximum number of references paged to storage in one operation
    */
   protected int downCacheSize = 2000;

   /**
    * Are we in paging mode?
    */
   protected boolean paging;

   /**
    * The page order value for the first reference paged in storage
    */
   protected long firstPagingOrder;
   
   /**
    * The value of page order for the next reference to page
    */
   protected long nextPagingOrder;
   
   protected MessageStore ms;
   
   /**
    * Constructor with default paging params.
    */
   public PagingChannelSupport(long channelID, MessageStore ms, PersistenceManager pm,
                               boolean recoverable, int maxSize)
   {
      super(channelID, pm, recoverable, maxSize);
      
      downCache = new ArrayList(downCacheSize);    
      
      this.ms = ms;
   }
   
   /**
    * Constructor specifying paging params.
    */
   public PagingChannelSupport(long channelID, MessageStore ms, PersistenceManager pm,
                               boolean recoverable, int maxSize,
                               int fullSize, int pageSize, int downCacheSize)
   {
      super(channelID, pm, recoverable, maxSize);
      
      if (pageSize >= fullSize)
      {
         throw new IllegalArgumentException("pageSize must be less than full size " + pageSize + ", " + fullSize);
      }
      if (downCacheSize > pageSize)
      {
         throw new IllegalArgumentException("pageSize cannot be smaller than downCacheSize");
      }
      if (pageSize <= 0)
      {
         throw new IllegalArgumentException("pageSize must be greater than zero");
      }
      if (downCacheSize <= 0)
      {
         throw new IllegalArgumentException("downCacheSize must be greater than zero");
      }
      
      downCache = new ArrayList(downCacheSize);    
      
      this.fullSize = fullSize;
      
      this.pageSize = pageSize;
      
      this.downCacheSize = downCacheSize;
      
      this.ms = ms;
   }
    
   // Receiver implementation ----------------------------------------------------------------------

   // Channel implementation -----------------------------------------------------------------------

   public int getMessageCount()
   {   
      int count = super.getMessageCount();
      
      // Also need to add the paged refs
      
      synchronized (lock)
      {      
         count += nextPagingOrder - firstPagingOrder;
      }
      
      return count;
   }
   
   // Public ---------------------------------------------------------------------------------------

   //Only used in testing
   public int downCacheCount()
   {
      synchronized (lock)
      {
         return downCache.size();
      }
   }

   //Only used in testing
   public boolean isPaging()
   {
      synchronized (lock)
      {
         return paging;
      }
   }
   
   public void setPagingParams(int fullSize, int pageSize, int downCacheSize)
   {
      synchronized (lock)
      { 
         if (active)
         {
            throw new IllegalStateException("Cannot set paging params when active");
         }
         
         this.fullSize = fullSize;
         
         this.pageSize = pageSize;
         
         this.downCacheSize = downCacheSize;         
      }
   }
   
   public void load() throws Exception
   {            
      synchronized (lock)
      {
         if (active)
         {
            throw new IllegalStateException("Cannot load channel when active");
         }
         
         if (trace) { log.trace(this + " loading channel state"); }
         
         unload();
         
         //Load the unpaged references
         InitialLoadInfo ili = pm.loadFromStart(channelID, fullSize);
         
         doLoad(ili);
         
         //Maybe we need to load some paged refs
         
         while (checkLoad()) {}
      }
   }
   
   public void unload() throws Exception
   {
      synchronized (lock)
      {
         if (active)
         {
            throw new IllegalStateException("Cannot unload channel when active");
         }
         
         messageRefs.clear();
         
         downCache.clear();
         
         paging = false;
         
         pm.setPaging(channelID, false);         
         
         firstPagingOrder = nextPagingOrder = 0;  
         
         clearAllScheduledDeliveries();
      }
   }
   
   
   // Protected ------------------------------------------------------------------------------------
   
   protected void loadPagedReferences(int number) throws Exception
   {
      if (trace) { log.trace(this + " Loading " + number + " paged references from storage"); }
  
      // Must flush the down cache first
      flushDownCache();
      
      List refInfos = pm.getPagedReferenceInfos(channelID, firstPagingOrder, number);      
      
      Map refMap = processReferences(refInfos);

      boolean loadedReliable = false;

      List toRemove = new ArrayList();
      
      int unreliableNumber = 0;

      Iterator iter = refInfos.iterator();
      while (iter.hasNext())
      {
         ReferenceInfo info = (ReferenceInfo)iter.next();
         
         MessageReference ref = addFromRefInfo(info, refMap);
         
         if (recoverable && ref.getMessage().isReliable())
         {
            loadedReliable = true;
         }
         else
         {
            // We put the non reliable refs (or reliable in a non-recoverable store)
            // in a list to be removed
            toRemove.add(ref);
            
            unreliableNumber++;
         }
      }
      
      if (!toRemove.isEmpty())
      {
         // Now we remove the references we loaded (only the non persistent or persistent in a non-recoverable store)
         
         pm.removeDepagedReferences(channelID, toRemove);
      }

      if (loadedReliable)
      {
         // If we loaded any reliable refs then we must set the page ordering to null in
         // the store otherwise they may get loaded again, the next time we do a load
         // We can't delete them since they're reliable and haven't been acked yet
            
         pm.updateReferencesNotPagedInRange(channelID, firstPagingOrder, firstPagingOrder + number - 1, number - unreliableNumber);
      }
            
      firstPagingOrder += number;
      
      if (firstPagingOrder == nextPagingOrder)
      {
         //No more refs in storage
         firstPagingOrder = nextPagingOrder = 0;
         
         if (messageRefs.size() != fullSize)
         {
            paging = false;
            
            pm.setPaging(channelID, false);            
         }
      }    
   }
      
   protected void cancelInternal(MessageReference ref) throws Exception
   {
      synchronized (lock)
      {         
         super.cancelInternal(ref);
         
         if (paging)
         {
            // if paging and the in memory queue is exactly full we need to evict the end reference to storage to
            // preserve the number of refs in the queue
            if (messageRefs.size() == fullSize + 1)
            {
               MessageReference refCancel = (MessageReference)messageRefs.removeLast();
    
               addToDownCache(refCancel, true);
            }
         }
               
         if (trace) { log.trace(this + " added " + ref + " back into state"); }      
      }
   }
      
   protected MessageReference removeFirstInMemory() throws Exception
   {
      MessageReference result = super.removeFirstInMemory();

      checkLoad();

      return result;
   }
   
   private boolean checkLoad() throws Exception
   {
      long refNum = nextPagingOrder - firstPagingOrder;
      
      if (refNum > 0)
      {
         int numberLoadable = (int)Math.min(refNum, pageSize);
         
         if (messageRefs.size() <= fullSize - numberLoadable)
         {
            //This will flush the down cache too
            loadPagedReferences(numberLoadable);
            
            return true;
         }
         else
         {
            return false;
         }
      }
      else
      {
         paging = false;
         
         pm.setPaging(channelID, false);
         
         return false;
      }
   }
    
   protected void addReferenceInMemory(MessageReference ref) throws Exception
   {     
      if (paging)
      {
         addToDownCache(ref, false);
      }
      else
      {
         super.addReferenceInMemory(ref);
         
         if (messageRefs.size() == fullSize)
         {
            // We are full in memory - go into paging mode
            if (trace) { log.trace(this + " going into paging mode"); }

            paging = true;
            
            pm.setPaging(channelID, true);            
         }
      }      
   }
   
   protected void addToDownCache(MessageReference ref, boolean cancelling) throws Exception
   {
      // If the down cache exists then refs are not spilled over immediately,
      // but store in the cache and spilled over in one go when the next load is requested,
      // or when it is full

      // Both non reliable and reliable references can go in the down cache,
      // however only non-reliable
      // references actually get added to storage, reliable references instead
      // just get their page ordering column updated since they will already be in storage

      //If cancelling then the ref is supposed to go back on the front of the queue segment in storage
      //so we set the page ordering to be firstPageOrdering - 1
      //If not cancelling, then the ref should go on the end of the queue in storage so
      //we set the page ordering to be nextPageOrdering
      
      if (cancelling)
      {
         ref.setPagingOrder(firstPagingOrder - 1);
         
         firstPagingOrder--;
      }
      else
      {
         ref.setPagingOrder(nextPagingOrder);
         
         nextPagingOrder++;
      }
      
      downCache.add(ref);

      if (trace) { log.trace(ref + " sent to downcache"); }
      
      if (downCache.size() == downCacheSize)
      {
         if (trace) { log.trace(this + "'s downcache is full (" + downCache.size() + " messages)"); }
               
         flushDownCache();
      }
   }

   protected void flushDownCache() throws Exception
   {
      if (trace) { log.trace(this + " flushing " + downCache.size() + " refs from downcache"); }

      // Non persistent refs won't already be in the db so they need to be inserted
      // Persistent refs in a recoverable state will already be there so need to be updated

      List toUpdate = new ArrayList();

      List toAdd = new ArrayList();

      Iterator iter = downCache.iterator();
      
      while (iter.hasNext())
      {
         MessageReference ref = (MessageReference) iter.next();
         
         if (ref.getMessage().isReliable() && recoverable)
         {
            toUpdate.add(ref);
         }
         else
         {
            toAdd.add(ref);
         }
      }
      
      if (!toAdd.isEmpty())
      {
         pm.pageReferences(channelID, toAdd, true);
      }
      
      if (!toUpdate.isEmpty())
      {
         pm.updatePageOrder(channelID, toUpdate);
      }

      downCache.clear();         

      if (trace) { log.trace(this + " cleared downcache"); }
   }
   

   
   protected void doLoad(InitialLoadInfo ili) throws Exception
   {
      if (ili.getMaxPageOrdering() != null)            
      {
         firstPagingOrder = ili.getMinPageOrdering().longValue();
         
         nextPagingOrder = ili.getMaxPageOrdering().longValue() + 1;
                           
         paging = true;    
      }
      else
      {
         firstPagingOrder = nextPagingOrder = 0;
         
         paging = false;
      }
            
      pm.setPaging(channelID, paging);     
      
      Map refMap = processReferences(ili.getRefInfos());
      
      Iterator iter = ili.getRefInfos().iterator();
      while (iter.hasNext())
      {
         ReferenceInfo info = (ReferenceInfo)iter.next();
         
         addFromRefInfo(info, refMap);
      }
   }
        
   // Private --------------------------------------------------------------------------------------
   
   protected MessageReference addFromRefInfo(ReferenceInfo info, Map refMap)
   {
      long msgId = info.getMessageId();

      MessageReference ref = (MessageReference)refMap.get(new Long(msgId));

      ref.setDeliveryCount(info.getDeliveryCount());

      ref.setPagingOrder(-1);
      
      ref.setScheduledDeliveryTime(info.getScheduledDelivery());
      
      ref.getMessage().incrementPersistentCount();
      
      //Schedule the delivery if necessary, or just add to the in memory queue
      if (!checkAndSchedule(ref))
      {
         messageRefs.addLast(ref, ref.getMessage().getPriority());
      }
      
      return ref;
   }
   

   protected Map processReferences(List refInfos) throws Exception
   {
      Map<Long, MessageReference> refMap = new HashMap<Long, MessageReference>(refInfos.size());

      List<Long> msgIdsToLoad = new ArrayList<Long>(refInfos.size());

      Iterator iter = refInfos.iterator();

      // Put the refs that we already have messages for in a map
      while (iter.hasNext())
      {
         ReferenceInfo info = (ReferenceInfo) iter.next();

         long msgId = info.getMessageId();

         MessageReference ref = ms.reference(msgId);

         if (ref != null)
         {
            refMap.put(msgId, ref);
         }
         else
         {
            // Add id to list of msg ids to load
            msgIdsToLoad.add(msgId);
         }
      }

      // Load the messages (if any)
      List messages = null;
      if (!msgIdsToLoad.isEmpty())
      {
         messages = pm.getMessages(msgIdsToLoad);

         if (messages.size() != msgIdsToLoad.size())
         {
            // Sanity check
            
            throw new IllegalStateException("Did not load correct number of messages, wanted:" +
                                            msgIdsToLoad.size() + " but got:" +
                                            messages.size());            
         }

         // Create references for these messages and add them to the reference map
         iter = messages.iterator();

         while (iter.hasNext())
         {
            Message m = (Message)iter.next();
            
            MessageReference ref = ms.reference(m);

            refMap.put(new Long(m.getMessageID()), ref);
         }
      }
      
      return refMap;
   }  
  
}
