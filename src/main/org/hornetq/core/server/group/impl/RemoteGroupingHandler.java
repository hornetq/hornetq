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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.utils.TypedProperties;

/**
 * A remote Grouping handler. This will use management notifications to communicate with the node that has the Local
 * Grouping handler to make proposals.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class RemoteGroupingHandler implements GroupingHandler
{
   private static Logger log = Logger.getLogger(RemoteGroupingHandler.class.getName());

   private final SimpleString name;

   private final ManagementService managementService;

   private final SimpleString address;

   private final Map<SimpleString, Response> responses = new ConcurrentHashMap<SimpleString, Response>();

   private final Lock lock = new ReentrantLock();

   private final Condition sendCondition = lock.newCondition();

   private final int timeout;

   private final ConcurrentMap<SimpleString, List<SimpleString>> groupMap = new ConcurrentHashMap<SimpleString, List<SimpleString>>();

   public RemoteGroupingHandler(final ManagementService managementService,
                                final SimpleString name,
                                final SimpleString address,
                                final int timeout)
   {
      this.name = name;
      this.address = address;
      this.managementService = managementService;
      this.timeout = timeout;
   }

   public SimpleString getName()
   {
      return name;
   }

   public Response propose(final Proposal proposal) throws Exception
   {
      // sanity check in case it is already selected
      Response response = responses.get(proposal.getGroupId());
      if (response != null)
      {
         return response;
      }

      try
      {
         lock.lock();

         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID, proposal.getGroupId());

         props.putSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE, proposal.getClusterName());

         props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, BindingType.LOCAL_QUEUE_INDEX);

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, 0);

         Notification notification = new Notification(null, NotificationType.PROPOSAL, props);

         managementService.sendNotification(notification);

         sendCondition.await(timeout, TimeUnit.MILLISECONDS);

         response = responses.get(proposal.getGroupId());

      }
      finally
      {
         lock.unlock();
      }
      if (response == null)
      {
         throw new IllegalStateException("no response received from group handler for " + proposal.getGroupId());
      }
      return response;
   }

   public Response getProposal(final SimpleString fullID)
   {
      return responses.get(fullID);
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
         sendCondition.signal();
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

   public void send(final Response response, final int distance) throws Exception
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
