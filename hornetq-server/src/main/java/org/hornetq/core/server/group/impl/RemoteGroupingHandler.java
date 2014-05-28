/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.core.server.group.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.TypedProperties;

/**
 * A remote Grouping handler.
 * <p/>
 * This will use management notifications to communicate with the node that has the Local Grouping
 * handler to make proposals.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public final class RemoteGroupingHandler extends GroupHandlingAbstract
{
   private final SimpleString name;

   private final Map<SimpleString, Response> responses = new ConcurrentHashMap<SimpleString, Response>();

   private final Lock lock = new ReentrantLock();

   private final Condition sendCondition = lock.newCondition();

   private final long timeout;

   private final long groupTimeout;

   private final ConcurrentMap<SimpleString, List<SimpleString>> groupMap = new ConcurrentHashMap<SimpleString, List<SimpleString>>();

   private final ConcurrentHashSet<Notification> pendingNotifications = new ConcurrentHashSet();

   private boolean started = false;

   public RemoteGroupingHandler(final ExecutorFactory executorFactory,
                                final ManagementService managementService,
                                final SimpleString name,
                                final SimpleString address,
                                final long timeout,
                                final long groupTimeout)
   {
      super(executorFactory != null ? executorFactory.getExecutor() : null, managementService, address);
      this.name = name;
      this.timeout = timeout;
      this.groupTimeout = groupTimeout;
   }

   public RemoteGroupingHandler(final ManagementService managementService,
                                final SimpleString name,
                                final SimpleString address,
                                final long timeout,
                                final long groupTimeout)
   {
      this(null, managementService, name, address, timeout, groupTimeout);
   }

   public SimpleString getName()
   {
      return name;
   }

   @Override
   public void start() throws Exception
   {
      if (started)
         return;
      started = true;
   }

   @Override
   public void stop() throws Exception
   {
      started = false;
   }

   @Override
   public boolean isStarted()
   {
      return started;
   }

   public void resendPending() throws Exception
   {
      // In case the RESET wasn't sent yet to the remote node, we may eventually miss a node send,
      // on that case the cluster-reset information will ask the group to resend any pending information

      try
      {
         lock.lock();

         for (Notification notification : pendingNotifications)
         {
            managementService.sendNotification(notification);
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   public Response propose(final Proposal proposal) throws Exception
   {
      // return it from the cache first
      Response response = responses.get(proposal.getGroupId());
      if (response != null)
      {
         checkTimeout(response);
         return response;
      }

      if (!started)
      {
         throw HornetQMessageBundle.BUNDLE.groupWhileStopping();
      }

      Notification notification = null;
      try
      {

         lock.lock();

         notification = createProposalNotification(proposal.getGroupId(), proposal.getClusterName());

         pendingNotifications.add(notification);

         managementService.sendNotification(notification);


         long timeLimit = System.currentTimeMillis() + timeout;

         do
         {
            sendCondition.await(timeout, TimeUnit.MILLISECONDS);

            response = responses.get(proposal.getGroupId());

            // You could have this response being null if you had multiple threads calling propose
            if (response != null)
            {
               break;
            }
         }
         while (timeLimit > System.currentTimeMillis());
      }
      finally
      {
         if (notification != null)
         {
            pendingNotifications.remove(notification);
         }
         lock.unlock();
      }
      if (response == null)
      {
         HornetQServerLogger.LOGGER.groupHandlerSendTimeout();
      }
      return response;
   }

   @Override
   public void awaitBindings()
   {
      // NO-OP
   }


   private void checkTimeout(Response response)
   {
      if (response != null)
      {
         if (groupTimeout > 0 && ((response.getTimeUsed() + groupTimeout) < System.currentTimeMillis()))
         {
            // We just touch the group on the local server at the half of the timeout
            // to avoid the group from expiring
            response.use();
            try
            {
               managementService.sendNotification(createProposalNotification(response.getGroupId(), response.getClusterName()));
            }
            catch (Exception ignored)
            {
            }
         }
      }
   }

   private Notification createProposalNotification(SimpleString groupId, SimpleString clusterName)
   {
      TypedProperties props = new TypedProperties();

      props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID, groupId);

      props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE, clusterName);

      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, BindingType.LOCAL_QUEUE_INDEX);

      props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);

      props.putIntProperty(ManagementHelper.HDR_DISTANCE, 0);

      return new Notification(null, NotificationType.PROPOSAL, props);
   }

   public Response getProposal(final SimpleString fullID, boolean touchTime)
   {
      Response response = responses.get(fullID);

      if (touchTime)
      {
         checkTimeout(response);
      }
      return response;
   }

   @Override
   public void remove(SimpleString groupid, SimpleString clusterName) throws Exception
   {
      List<SimpleString> groups = groupMap.get(clusterName);
      if (groups != null)
      {
         groups.remove(groupid);
      }
      responses.remove(groupid);
      fireUnproposed(groupid);
   }

   @Override
   public void remove(SimpleString groupid, SimpleString clusterName, int distance) throws Exception
   {
      remove(groupid, clusterName);

      sendUnproposal(groupid, clusterName, distance);
   }

   public void proposed(final Response response) throws Exception
   {
      try
      {
         lock.lock();
         responses.put(response.getGroupId(), response);
         List<SimpleString> newList = new ArrayList<SimpleString>();
         List<SimpleString> oldList = groupMap.putIfAbsent(response.getChosenClusterName(), newList);
         if (oldList != null)
         {
            newList = oldList;
         }
         newList.add(response.getGroupId());
         // We could have more than one Requests waiting in case you have multiple producers
         // using different groups
         sendCondition.signalAll();
      }
      finally
      {
         lock.unlock();
      }
   }

   public Response receive(final Proposal proposal, final int distance) throws Exception
   {
      TypedProperties props = new TypedProperties();
      props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID, proposal.getGroupId());
      props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE, proposal.getClusterName());
      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, BindingType.LOCAL_QUEUE_INDEX);
      props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);
      props.putIntProperty(ManagementHelper.HDR_DISTANCE, distance);
      Notification notification = new Notification(null, NotificationType.PROPOSAL, props);
      managementService.sendNotification(notification);
      return null;
   }

   public void sendProposalResponse(final Response response, final int distance) throws Exception
   {
      // NO-OP
   }

   public void addGroupBinding(final GroupBinding groupBinding)
   {
      // NO-OP
   }

   public void onNotification(final Notification notification)
   {
      // removing the groupid if the binding has been removed
      if (notification.getType() == NotificationType.BINDING_REMOVED)
      {
         SimpleString clusterName = notification.getProperties()
            .getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);
         List<SimpleString> list = groupMap.remove(clusterName);
         if (list != null)
         {
            for (SimpleString val : list)
            {
               if (val != null)
               {
                  responses.remove(val);
               }
            }
         }
      }
   }
}
