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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.plugin.contract.PostOffice;
import org.jboss.messaging.util.Future;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A LocalClusteredQueue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class LocalClusteredQueue extends PagingFilteredQueue implements ClusteredQueue
{
   private PostOfficeInternal office;
   
   private volatile int lastCount;
   
   private volatile boolean changedSignificantly;
   
   private RemoteQueueStub pullQueue;
   
   private String nodeId;
 
   //TODO - we shouldn't have to specify office AND nodeId
   public LocalClusteredQueue(PostOffice office, String nodeId, String name, long id, MessageStore ms, PersistenceManager pm,             
                              boolean acceptReliableMessages, boolean recoverable, QueuedExecutor executor,
                              Filter filter,
                              int fullSize, int pageSize, int downCacheSize)
   {
      super(name, id, ms, pm, acceptReliableMessages, recoverable, executor, filter, fullSize, pageSize, downCacheSize);
     
      this.nodeId = nodeId;
      
      //FIXME - this cast is a hack
      this.office = (PostOfficeInternal)office;
   }
   
   public LocalClusteredQueue(PostOffice office, String nodeId, String name, long id, MessageStore ms, PersistenceManager pm,             
                              boolean acceptReliableMessages, boolean recoverable, QueuedExecutor executor,
                              Filter filter)
   {
      super(name, id, ms, pm, acceptReliableMessages, recoverable, executor, filter);
      
      this.nodeId = nodeId;
      
      //FIXME - this cast is a hack
      this.office = (PostOfficeInternal)office;
   }
   
   public void setPullQueue(RemoteQueueStub queue)
   {
      this.pullQueue = queue;
   }
   
   public QueueStats getStats()
   {      
      int cnt = messageCount();
      
      if (cnt != lastCount)
      {
         changedSignificantly = true;
         
         lastCount = cnt;
      }
      
      return new QueueStats(name, cnt);
   }      
   
   //Have the stats changed significantly since the last time we request them?
   public boolean changedSignificantly()
   {
      return changedSignificantly;
   }
   
   public boolean isLocal()
   {
      return true;
   }
     
   public String getNodeId()
   {
      return nodeId;
   }
      
   /*
    * Used when pulling messages from a remote queue
    */
   public List getDeliveries(int number) throws Exception
   {
      List dels = new ArrayList();
      
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            //We only get the refs if receiversReady = false so as not to steal messages that
            //might be consumed by local receivers            
            if (!receiversReady)
            {
               MessageReference ref;
               
               while ((ref = removeFirstInMemory()) != null)
               {
                  SimpleDelivery del = new SimpleDelivery(this, ref);
                  
                  deliveries.add(del);
                  
                  dels.add(del);               
               }           
               return dels;
            }
            else
            {
               return Collections.EMPTY_LIST;
            }
         }
      }          
   }
   
   /*
    * This is the same as the normal handle() method on the Channel except it doesn't
    * persist the message even if it is persistent - this is because persistent messages
    * are always persisted on the sending node before sending.
    */
   public Delivery handleFromCluster(MessageReference ref)
      throws Exception
   {
      if (filter != null && !filter.accept(ref))
      {
         Delivery del = new SimpleDelivery(this, ref, true, false);
         
         return del;
      }
      
      checkClosed();
      
      Future result = new Future();
      
      // Instead of executing directly, we add the handle request to the event queue.
      // Since remoting doesn't currently handle non blocking IO, we still have to wait for the
      // result, but when remoting does, we can use a full SEDA approach and get even better
      // throughput.
      this.executor.execute(new HandleRunnable(result, null, ref, false));

      return (Delivery)result.getResult();
   }
   
   public void acknowledgeFromCluster(Delivery d) throws Throwable
   {
      acknowledgeInternal(d, false);      
   }
   
   protected void deliverInternal(boolean handle) throws Throwable
   {
      super.deliverInternal(handle);
      
      if (!handle)
      {
         if (receiversReady)
         {
            //Delivery has been prompted (not from handle call)
            //and has run, and there are consumers that are still interested in receiving more
            //refs but there are none available in the channel (either the channel is empty
            //or there are only refs that don't match any selectors)
            //then we should perhaps pull some messages from a remote queue
            if (pullQueue != null)
            {
               office.pullMessages(this, pullQueue);                        
            }
         }
      }
   }
   
   public boolean isClustered()
   {
      return true;
   }
}
