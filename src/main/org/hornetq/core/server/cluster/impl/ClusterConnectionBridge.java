/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.server.cluster.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.MessageFlowRecord;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.utils.UUID;
import org.hornetq.utils.UUIDGenerator;

/**
 * A ClusterConnectionBridge
 *
 * @author tim
 *
 *
 */
public class ClusterConnectionBridge extends BridgeImpl
{
   private static final Logger log = Logger.getLogger(ClusterConnectionBridge.class);

   private final MessageFlowRecord flowRecord;

   private final SimpleString managementAddress;

   private final SimpleString managementNotificationAddress;

   private ClientConsumer notifConsumer;

   private final SimpleString idsHeaderName;

   private final TransportConfiguration connector;

   private final String targetNodeID;

   public ClusterConnectionBridge(final ServerLocatorInternal serverLocator,
                                  final UUID nodeUUID,
                                  final String targetNodeID,
                                  final SimpleString name,
                                  final Queue queue,
                                  final Executor executor,
                                  final SimpleString filterString,
                                  final SimpleString forwardingAddress,
                                  final ScheduledExecutorService scheduledExecutor,
                                  final Transformer transformer,
                                  final boolean useDuplicateDetection,
                                  final String user,
                                  final String password,
                                  final boolean activated,
                                  final StorageManager storageManager,
                                  final SimpleString managementAddress,
                                  final SimpleString managementNotificationAddress,
                                  final MessageFlowRecord flowRecord,
                                  final TransportConfiguration connector) throws Exception
   {
      super(serverLocator,
            nodeUUID,
            name,
            queue,
            executor,
            filterString,
            forwardingAddress,
            scheduledExecutor,
            transformer,
            useDuplicateDetection,
            user,
            password,
            activated,
            storageManager);

      idsHeaderName = MessageImpl.HDR_ROUTE_TO_IDS.concat(name);

      this.targetNodeID = targetNodeID;
      this.managementAddress = managementAddress;
      this.managementNotificationAddress = managementNotificationAddress;
      this.flowRecord = flowRecord;
      this.connector = connector;
      
      // we need to disable DLQ check on the clustered bridges
      queue.setInternalQueue(true);
   }

   @Override
   protected ServerMessage beforeForward(ServerMessage message)
   {
      // We make a copy of the message, then we strip out the unwanted routing id headers and leave
      // only
      // the one pertinent for the address node - this is important since different queues on different
      // nodes could have same queue ids
      // Note we must copy since same message may get routed to other nodes which require different headers
      message = message.copy();

      // TODO - we can optimise this

      Set<SimpleString> propNames = new HashSet<SimpleString>(message.getPropertyNames());

      byte[] queueIds = message.getBytesProperty(idsHeaderName);

      for (SimpleString propName : propNames)
      {
         if (propName.startsWith(MessageImpl.HDR_ROUTE_TO_IDS))
         {
            message.removeProperty(propName);
         }
      }

      message.putBytesProperty(MessageImpl.HDR_ROUTE_TO_IDS, queueIds);

      message = super.beforeForward(message);

      return message;
   }

   private void setupNotificationConsumer() throws Exception
   {
      if (flowRecord != null)
      {
         flowRecord.reset();

         if (notifConsumer != null)
         {
            try
            {
               notifConsumer.close();

               notifConsumer = null;
            }
            catch (HornetQException e)
            {
               log.error("Failed to close consumer", e);
            }
         }

         // Get the queue data

         String qName = "notif." + UUIDGenerator.getInstance().generateStringUUID();

         SimpleString notifQueueName = new SimpleString(qName);

         SimpleString filter = new SimpleString(ManagementHelper.HDR_BINDING_TYPE + "<>" +
                                                BindingType.DIVERT.toInt() +
                                                " AND " +
                                                ManagementHelper.HDR_NOTIFICATION_TYPE +
                                                " IN ('" +
                                                NotificationType.BINDING_ADDED +
                                                "','" +
                                                NotificationType.BINDING_REMOVED +
                                                "','" +
                                                NotificationType.CONSUMER_CREATED +
                                                "','" +
                                                NotificationType.CONSUMER_CLOSED +
                                                "','" +
                                                NotificationType.PROPOSAL +
                                                "','" +
                                                NotificationType.PROPOSAL_RESPONSE +
                                                "') AND " +
                                                ManagementHelper.HDR_DISTANCE +
                                                "<" +
                                                flowRecord.getMaxHops() +
                                                " AND (" +
                                                ManagementHelper.HDR_ADDRESS +
                                                " LIKE '" +
                                                flowRecord.getAddress() +
                                                "%')");

         session.createQueue(managementNotificationAddress, notifQueueName, filter, false);

         notifConsumer = session.createConsumer(notifQueueName);

         notifConsumer.setMessageHandler(flowRecord);

         session.start();

         ClientMessage message = session.createMessage(false);

         ManagementHelper.putOperationInvocation(message,
                                                 ResourceNames.CORE_SERVER,
                                                 "sendQueueInfoToQueue",
                                                 notifQueueName.toString(),
                                                 flowRecord.getAddress());

         ClientProducer prod = session.createProducer(managementAddress);

         prod.send(message);
      }
   }

   @Override
   protected void afterConnect() throws Exception
   {
      setupNotificationConsumer();
   }
   
   @Override
   protected ClientSessionFactory createSessionFactory() throws Exception
   {
      //We create the session factory using the specified connector
      
      return serverLocator.createSessionFactory(connector);      
   }
   
   @Override
   public void connectionFailed(HornetQException me, boolean failedOver)
   {
      if (!failedOver && !session.isClosed())
      {
         try
         {
            session.cleanUp(true);
         }
         catch (Exception e)
         {
            log.warn("Unable to clean up the session after a connection failure", e);
         }
         serverLocator.notifyNodeDown(targetNodeID);
      }
      super.connectionFailed(me, failedOver);
   }
}
