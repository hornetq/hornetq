/*
 * Copyright 2009 Red Hat, Inc.
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

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUIDGenerator;

/**
 * A BroadcastGroupImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 15 Nov 2008 09:45:32
 *
 */
public class BroadcastGroupImpl implements BroadcastGroup, Runnable
{
   private static final Logger log = Logger.getLogger(BroadcastGroupImpl.class);

   private final String nodeID;

   private final String name;

   private final InetAddress localAddress;

   private final int localPort;

   private final InetAddress groupAddress;

   private final int groupPort;

   private DatagramSocket socket;

   private final List<Pair<TransportConfiguration, TransportConfiguration>> connectorPairs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

   private boolean started;

   private ScheduledFuture<?> future;

   private boolean active;

   // Each broadcast group has a unique id - we use this to detect when more than one group broadcasts the same node id
   // on the network which would be an error
   private final String uniqueID;

   private NotificationService notificationService;

   /**
    * Broadcast group is bound locally to the wildcard address
    */
   public BroadcastGroupImpl(final String nodeID,
                             final String name,
                             final InetAddress localAddress,
                             final int localPort,
                             final InetAddress groupAddress,
                             final int groupPort,
                             final boolean active) throws Exception
   {
      this.nodeID = nodeID;

      this.name = name;

      this.localAddress = localAddress;

      this.localPort = localPort;

      this.groupAddress = groupAddress;

      this.groupPort = groupPort;

      this.active = active;

      uniqueID = UUIDGenerator.getInstance().generateStringUUID();
   }

   public void setNotificationService(final NotificationService notificationService)
   {
      this.notificationService = notificationService;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      if (localPort != -1)
      {
         socket = new DatagramSocket(localPort, localAddress);
      }
      else
      {
         if (localAddress != null)
         {
            log.warn("local-bind-address specified for broadcast group but no local-bind-port specified so socket will NOT be bound " + 
                     "to a local address/port");
         }
         socket = new DatagramSocket();
      }

      started = true;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, NotificationType.BROADCAST_GROUP_STARTED, props);
         notificationService.sendNotification(notification);
      }
   }

   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }

      if (future != null)
      {
         future.cancel(false);
      }

      socket.close();

      started = false;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, NotificationType.BROADCAST_GROUP_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            BroadcastGroupImpl.log.warn("unable to send notification when broadcast group is stopped", e);
         }
      }

   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public String getName()
   {
      return name;
   }

   public synchronized void addConnectorPair(final Pair<TransportConfiguration, TransportConfiguration> connectorPair)
   {
      connectorPairs.add(connectorPair);
   }

   public synchronized void removeConnectorPair(final Pair<TransportConfiguration, TransportConfiguration> connectorPair)
   {
      connectorPairs.remove(connectorPair);
   }

   public synchronized int size()
   {
      return connectorPairs.size();
   }

   public synchronized void activate()
   {
      active = true;
   }

   public synchronized void broadcastConnectors() throws Exception
   {
      if (!active)
      {
         return;
      }

      HornetQBuffer buff = HornetQBuffers.dynamicBuffer(4096);

      buff.writeString(nodeID);

      buff.writeString(uniqueID);

      buff.writeInt(connectorPairs.size());

      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : connectorPairs)
      {
         connectorPair.a.encode(buff);

         if (connectorPair.b != null)
         {
            buff.writeBoolean(true);

            connectorPair.b.encode(buff);
         }
         else
         {
            buff.writeBoolean(false);
         }
      }

      byte[] data = buff.toByteBuffer().array();

      DatagramPacket packet = new DatagramPacket(data, data.length, groupAddress, groupPort);

      socket.send(packet);
   }

   public void run()
   {
      if (!started)
      {
         return;
      }

      try
      {
         broadcastConnectors();
      }
      catch (Exception e)
      {
         BroadcastGroupImpl.log.error("Failed to broadcast connector configs", e);
      }
   }

   public synchronized void setScheduledFuture(final ScheduledFuture<?> future)
   {
      this.future = future;
   }

}
