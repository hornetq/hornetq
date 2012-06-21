package org.hornetq.core.server.cluster.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.cluster.BroadcastEndpoint;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUIDGenerator;

public class PluggableBroadcastGroup implements BroadcastGroup, Runnable
{
   private static final Logger log = Logger.getLogger(PluggableBroadcastGroup.class);

   private final String nodeID;

   private final String name;

   private final List<TransportConfiguration> connectors = new ArrayList<TransportConfiguration>();

   private boolean started;

   private ScheduledFuture<?> future;

   private boolean active;

   private boolean loggedBroadcastException = false;

   // Each broadcast group has a unique id - we use this to detect when more than one group broadcasts the same node id
   // on the network which would be an error
   private final String uniqueID;

   private NotificationService notificationService;
   
   private long broadcastPeriod;

   private BroadcastEndpoint endpoint;

   /**
    * Broadcast group is bound locally to the wildcard address
    */
   public PluggableBroadcastGroup(final String nodeID,
                             final String name,
                             final boolean active,
                             final long broadcastPeriod,
                             final BroadcastEndpoint endpoint) throws Exception
   {
      this.nodeID = nodeID;

      this.name = name;

      this.active = active;
      
      this.broadcastPeriod = broadcastPeriod;
      
      this.endpoint = endpoint;

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
      
      endpoint.start(true);

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
      
      try
      {
         endpoint.stop();
      }
      catch (Exception e1)
      {
         log.warn("Exception in stopping endpoint " + endpoint, e1);
      }

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
            log.warn("Exception sending notification " + notification, e);
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

   public synchronized void addConnector(final TransportConfiguration tcConfig)
   {
      connectors.add(tcConfig);
   }

   public synchronized void removeConnector(final TransportConfiguration tcConfig)
   {
      connectors.remove(tcConfig);
   }

   public synchronized int size()
   {
      return connectors.size();
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

      buff.writeInt(connectors.size());

      for (TransportConfiguration tcConfig : connectors)
      {
         tcConfig.encode(buff);
      }

      byte[] data = buff.toByteBuffer().array();
      
      endpoint.broadcast(data);
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
         loggedBroadcastException = false;
      }
      catch (Exception e)
      {
         // only log the exception at ERROR level once, even if it fails multiple times in a row - HORNETQ-919
         if (!loggedBroadcastException)
         {
            log.error("Failed to broadcast connector configs", e);
            loggedBroadcastException = true;
         }
         else
         {
            log.error("Failed to broadcast connector configs...again", e);
         }
      }
   }

   public void schedule(ScheduledExecutorService scheduledExecutor)
   {
      future = scheduledExecutor.scheduleWithFixedDelay(this, 0, broadcastPeriod, TimeUnit.MILLISECONDS);
   }

}
