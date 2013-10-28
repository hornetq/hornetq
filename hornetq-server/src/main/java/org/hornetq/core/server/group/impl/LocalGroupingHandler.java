/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.core.server.group.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.TypedProperties;

/**
 * A Local Grouping handler. All the Remote handlers will talk with us
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public final class LocalGroupingHandler implements GroupingHandler
{
   private final ConcurrentHashMap<SimpleString, GroupBinding> map = new ConcurrentHashMap<SimpleString, GroupBinding>();

   private final ConcurrentHashMap<SimpleString, List<GroupBinding>> groupMap = new ConcurrentHashMap<SimpleString, List<GroupBinding>>();

   private final SimpleString name;

   private final ManagementService managementService;

   private final SimpleString address;

   private final StorageManager storageManager;

   private final long timeout;

   private final Lock lock = new ReentrantLock();

   private final Condition awaitCondition = lock.newCondition();

   private final List<SimpleString> bindingsAdded = new ArrayList<SimpleString>();

   private final long groupTimeout;

   private boolean waitingForBindings = false;

   private final Executor executor;

   private final ScheduledExecutorService scheduledExecutor;

   private boolean started;

   private GroupIdReaper reaperRunnable;

   private ScheduledFuture reaperFuture;

   private long reaperPeriod;

   public LocalGroupingHandler(final ExecutorFactory executorFactory,
                               final ScheduledExecutorService scheduledExecutor,
                               final ManagementService managementService,
                               final SimpleString name,
                               final SimpleString address,
                               final StorageManager storageManager,
                               final long timeout,
                               final long groupTimeout,
                               long reaperPeriod)
   {
      this.reaperPeriod = reaperPeriod;
      this.executor = executorFactory.getExecutor();
      this.scheduledExecutor = scheduledExecutor;
      this.managementService = managementService;
      this.name = name;
      this.address = address;
      this.storageManager = storageManager;
      this.timeout = timeout;
      this.groupTimeout = groupTimeout;
   }

   public SimpleString getName()
   {
      return name;
   }

   public Response propose(final Proposal proposal) throws Exception
   {
      OperationContext originalCtx = storageManager.getContext();

      try
      {
         // the waitCompletion cannot be done inside an ordered executor or we would starve when the thread pool is full
         storageManager.setContext(storageManager.newSingleThreadContext());

         if (proposal.getClusterName() == null)
         {
            GroupBinding original = map.get(proposal.getGroupId());
            return original == null ? null : new Response(proposal.getGroupId(), original.getClusterName());
         }
         GroupBinding groupBinding = new GroupBinding(proposal.getGroupId(), proposal.getClusterName());
         if (map.putIfAbsent(groupBinding.getGroupId(), groupBinding) == null)
         {
            groupBinding.setId(storageManager.generateUniqueID());
            List<GroupBinding> newList = new ArrayList<GroupBinding>();
            List<GroupBinding> oldList = groupMap.putIfAbsent(groupBinding.getClusterName(), newList);
            if (oldList != null)
            {
               newList = oldList;
            }
            newList.add(groupBinding);
            storageManager.addGrouping(groupBinding);
            if (!storageManager.waitOnOperations(timeout))
            {
               throw HornetQMessageBundle.BUNDLE.ioTimeout();
            }
            return new Response(groupBinding.getGroupId(), groupBinding.getClusterName());
         }
         else
         {
            groupBinding = map.get(proposal.getGroupId());
            return new Response(groupBinding.getGroupId(), proposal.getClusterName(), groupBinding.getClusterName());
         }
      }
      finally
      {
         storageManager.setContext(originalCtx);
      }
   }

   public void proposed(final Response response) throws Exception
   {
   }

   @Override
   public void remove(SimpleString groupid, SimpleString clusterName, int distance)
   {
   }

   public void send(final Response response, final int distance) throws Exception
   {
      TypedProperties props = new TypedProperties();
      props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID, response.getGroupId());
      props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE, response.getClusterName());
      props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_ALT_VALUE, response.getAlternativeClusterName());
      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, BindingType.LOCAL_QUEUE_INDEX);
      props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);
      props.putIntProperty(ManagementHelper.HDR_DISTANCE, distance);
      Notification notification = new Notification(null, NotificationType.PROPOSAL_RESPONSE, props);
      managementService.sendNotification(notification);
   }

   public Response receive(final Proposal proposal, final int distance) throws Exception
   {
      HornetQServerLogger.LOGGER.trace("received proposal " + proposal);
      return propose(proposal);
   }

   public void addGroupBinding(final GroupBinding groupBinding)
   {
      map.put(groupBinding.getGroupId(), groupBinding);
      List<GroupBinding> newList = new ArrayList<GroupBinding>();
      List<GroupBinding> oldList = groupMap.putIfAbsent(groupBinding.getClusterName(), newList);
      if (oldList != null)
      {
         newList = oldList;
      }
      newList.add(groupBinding);
   }

   public Response getProposal(final SimpleString fullID)
   {
      GroupBinding original = map.get(fullID);
      return original == null ? null : new Response(fullID, original.getClusterName());
   }

   @Override
   public void awaitBindings() throws Exception
   {
      if (groupMap.size() > 0)
      {
         try
         {
            lock.lock();
            waitingForBindings = true;
            //make a copy of the bindings added so far from the cluster via onNotification().
            List<SimpleString> bindingsAlreadyAdded = new ArrayList<SimpleString>(bindingsAdded);
            //clear the bindings
            bindingsAdded.clear();
            //now add all the group bindings that were loaded by the journal
            bindingsAdded.addAll(groupMap.keySet());
            //and if we remove persisted bindings from whats been added so far we have left any bindings we havent yet
            //received via onNotification
            bindingsAdded.removeAll(bindingsAlreadyAdded);
            //now we wait here for the rest to be received in onNotification, it will signal once all have been received.
            //if we arent signaled then bindingsAdded still has some groupids we need to remove.
            if(!awaitCondition.await(timeout, TimeUnit.MILLISECONDS))
            {
               //if we get here there are some bindings that we haven't been told about in the cluster so we need to remove them.
               for (SimpleString clusterName : groupMap.keySet())
               {
                  if(bindingsAdded.contains(clusterName))
                  {
                     removeGrouping(clusterName, true);
                  }
               }
            }
         }
         finally
         {
            waitingForBindings = false;
            lock.unlock();
         }
      }
   }

   public void onNotification(final Notification notification)
   {
      if (notification.getType() == NotificationType.BINDING_REMOVED)
      {
         SimpleString clusterName = notification.getProperties()
                                                .getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);
         removeGrouping(clusterName, false);
      }
      else if(notification.getType() == NotificationType.BINDING_ADDED)
      {
         SimpleString clusterName = notification.getProperties()
               .getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);
         try
         {
            lock.lock();
            //if we are not yet waiting for bindings then add any new ones.
            if(!waitingForBindings && !bindingsAdded.contains(clusterName))
            {
               bindingsAdded.add(clusterName);
            }
            //we are now waiting for bindings in awaitBindings() so remove
            else
            {
               bindingsAdded.remove(clusterName);
            }
            //and signal once we have heard from all of them.
            if(waitingForBindings && bindingsAdded.size() == 0)
            {
               awaitCondition.signal();
            }
         }
         finally
         {
            lock.unlock();
         }
      }
   }
   public synchronized void start() throws Exception
   {
      if (started)
         return;
      if (reaperPeriod > 0 && groupTimeout > 0)
      {
         if (reaperFuture != null)
         {
            reaperFuture.cancel(true);
            reaperFuture = null;
         }

         reaperRunnable = new GroupIdReaper();

         reaperFuture = scheduledExecutor.scheduleAtFixedRate(reaperRunnable, reaperPeriod,
                                                              reaperPeriod, TimeUnit.MILLISECONDS);
      }
      started = true;
   }

   public synchronized void stop() throws Exception
   {
      started = false;
      if (reaperFuture != null)
      {
         reaperFuture.cancel(true);
         reaperFuture = null;
      }
   }

   public boolean isStarted()
   {
      return started;
   }

   private void removeGrouping(final SimpleString clusterName, final boolean warn)
   {
      final List<GroupBinding> list = groupMap.remove(clusterName);
      if(warn && list != null)
      {
         HornetQServerLogger.LOGGER.groupingQueueRemoved(list.size(), clusterName);
      }
      if (list != null)
      {
         executor.execute(new Runnable()
         {
            @Override
            public void run()
            {
               for (GroupBinding val : list)
               {
                  if (val != null)
                  {

                     map.remove(val.getGroupId());

                     try
                     {
                        storageManager.deleteGrouping(val, map.isEmpty());
                     }
                     catch (Exception e)
                     {
                        HornetQServerLogger.LOGGER.unableToDeleteGroupBindings(e, val.getGroupId());
                     }
                  }
               }
               if(warn)
               {
                  HornetQServerLogger.LOGGER.groupingQueueRemovedComplete(clusterName);
               }
            }
         });

      }
   }

   private final class GroupIdReaper implements Runnable
   {
      public void run()
      {
         // The reaper thread should be finished case the PostOffice is gone
         // This is to avoid leaks on PostOffice between stops and starts
         if (isStarted())
         {
            if (!isStarted())
               return;

            for (GroupBinding groupBinding : map.values())
            {
               if(groupBinding.getTimeCreated() + groupTimeout > System.currentTimeMillis())
               {
                  map.remove(groupBinding.getGroupId());
                  List<GroupBinding> groupBindings = groupMap.get(groupBinding.getClusterName());
                  groupBindings.remove(groupBinding);
                  TypedProperties props = new TypedProperties();
                  props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID, groupBinding.getGroupId());
                  props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE, groupBinding.getClusterName());
                  props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, BindingType.LOCAL_QUEUE_INDEX);
                  props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);
                  props.putIntProperty(ManagementHelper.HDR_DISTANCE, 0);
                  Notification notification = new Notification(null, NotificationType.UNPROPOSAL, props);
                  try
                  {
                     managementService.sendNotification(notification);
                  }
                  catch (Exception e)
                  {
                     HornetQServerLogger.LOGGER.errorHandlingMessage(e);
                  }
                  try
                  {
                     storageManager.deleteGrouping(groupBinding, true);
                  }
                  catch (Exception e)
                  {
                     HornetQServerLogger.LOGGER.unableToDeleteGroupBindings(e, groupBinding.getGroupId());
                  }
               }
            }
         }
      }
   }
}
