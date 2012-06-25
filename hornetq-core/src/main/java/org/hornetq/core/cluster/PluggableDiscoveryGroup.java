package org.hornetq.core.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.server.HornetQLogger;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.utils.TypedProperties;

public class PluggableDiscoveryGroup implements DiscoveryGroup, Runnable
{
   private static final boolean isTrace = HornetQLogger.LOGGER.isTraceEnabled();

   private final List<DiscoveryListener> listeners = new ArrayList<DiscoveryListener>();

   private final String name;

   private Thread thread;

   private boolean received;

   private final Object waitLock = new Object();

   private final Map<String, DiscoveryEntry> connectors = new ConcurrentHashMap<String, DiscoveryEntry>();

   private final long timeout;

   private volatile boolean started;

   private final String nodeID;

   private final Map<String, String> uniqueIDMap = new HashMap<String, String>();

   private final BroadcastEndpoint endpoint;
   
   private final NotificationService notificationService;

   public PluggableDiscoveryGroup(final String nodeID, final String name, final long timeout,
                                  BroadcastEndpoint endpoint,
                                  NotificationService service) throws Exception
   {
      this.nodeID = nodeID;
      this.name = name;
      this.timeout = timeout;
      this.endpoint = endpoint;
      this.notificationService = service;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      endpoint.start(false);

      started = true;

      thread = new Thread(this, "hornetq-discovery-group-thread-" + name);

      thread.setDaemon(true);

      thread.start();

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));

         Notification notification = new Notification(nodeID, NotificationType.DISCOVERY_GROUP_STARTED, props);

         notificationService.sendNotification(notification);
      }
   }

   public void stop()
   {
      synchronized (this)
      {
         if (!started)
         {
            return;
         }

         started = false;
      }

      synchronized (waitLock)
      {
         waitLock.notifyAll();
      }
      
      try
      {
         endpoint.stop();
      }
      catch (Exception e1)
      {
         HornetQLogger.LOGGER.warn("Exception stopping endpoint: " + endpoint, e1);
      }

      try
      {
         thread.interrupt();
         thread.join(10000);
         if(thread.isAlive())
         {
            HornetQLogger.LOGGER.timedOutStoppingDiscovery();
         }
      }
      catch (InterruptedException e)
      {
      }

      thread = null;

      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, NotificationType.DISCOVERY_GROUP_STOPPED, props);
         try
         {
            notificationService.sendNotification(notification);
         }
         catch (Exception e)
         {
            HornetQLogger.LOGGER.errorSendingNotifOnDiscoveryStop(e);
         }
      }
   }

   public boolean isStarted()
   {
      return started;
   }

   public String getName()
   {
      return name;
   }

   public synchronized List<DiscoveryEntry> getDiscoveryEntries()
   {
      List<DiscoveryEntry> list = new ArrayList<DiscoveryEntry>();

      list.addAll(connectors.values());

      return list;
   }

   public boolean waitForBroadcast(final long timeout)
   {
      synchronized (waitLock)
      {
         long start = System.currentTimeMillis();

         long toWait = timeout;

         while (started && !received && (toWait > 0 || timeout == 0))
         {
            try
            {
               waitLock.wait(toWait);
            }
            catch (InterruptedException e)
            {
            }

            if (timeout != 0)
            {
               long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }
         }

         boolean ret = received;

         received = false;

         return ret;
      }
   }

   /*
    * This is a sanity check to catch any cases where two different nodes are broadcasting the same node id either
    * due to misconfiguration or problems in failover
    */
   private void checkUniqueID(final String originatingNodeID, final String uniqueID)
   {
      String currentUniqueID = uniqueIDMap.get(originatingNodeID);

      if (currentUniqueID == null)
      {
         uniqueIDMap.put(originatingNodeID, uniqueID);
      }
      else
      {
         if (!currentUniqueID.equals(uniqueID))
         {
            HornetQLogger.LOGGER.multipleServersBroadcastingSameNode(originatingNodeID);
            uniqueIDMap.put(originatingNodeID, uniqueID);
         }
      }
   }

   public void run()
   {
      try
      {
         byte[] data = null;

         while (true)
         {
            if (!started)
            {
               return;
            }

            try
            {
               data = endpoint.receiveBroadcast();
            }
            catch (Exception e)
            {
               if (!started)
               {
                  return;
               }
               else
               {
                  HornetQLogger.LOGGER.errorReceivingPAcketInDiscovery(e);
               }
            }

            HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(data);

            String originatingNodeID = buffer.readString();

            String uniqueID = buffer.readString();

            checkUniqueID(originatingNodeID, uniqueID);

            if (nodeID.equals(originatingNodeID))
            {
               if (checkExpiration())
               {
                  callListeners();
               }
               // Ignore traffic from own node
               continue;
            }

            int size = buffer.readInt();

            boolean changed = false;

            synchronized (this)
            {
               for (int i = 0; i < size; i++)
               {
                  TransportConfiguration connector = new TransportConfiguration();

                  connector.decode(buffer);

                  DiscoveryEntry entry = new DiscoveryEntry(originatingNodeID, connector, System.currentTimeMillis());

                  DiscoveryEntry oldVal = connectors.put(originatingNodeID, entry);

                  if (oldVal == null)
                  {
                     changed = true;
                  }
               }

               changed = changed || checkExpiration();
            }

            if (changed)
            {
               if (isTrace)
               {
                  HornetQLogger.LOGGER.trace("Connectors changed on Discovery:");
                  for (DiscoveryEntry connector : connectors.values())
                  {
                     HornetQLogger.LOGGER.trace(connector);
                  }
               }
               callListeners();
            }

            synchronized (waitLock)
            {
               received = true;

               waitLock.notifyAll();
            }
         }
      }
      catch (Exception e)
      {
         HornetQLogger.LOGGER.failedToReceiveDatagramInDiscovery(e);
      }
   }

   public synchronized void registerListener(final DiscoveryListener listener)
   {
      listeners.add(listener);

      if (!connectors.isEmpty())
      {
         listener.connectorsChanged();
      }
   }

   public synchronized void unregisterListener(final DiscoveryListener listener)
   {
      listeners.remove(listener);
   }

   private void callListeners()
   {
      for (DiscoveryListener listener : listeners)
      {
         try
         {
            listener.connectorsChanged();
         }
         catch (Throwable t)
         {
            // Catch it so exception doesn't prevent other listeners from running
            HornetQLogger.LOGGER.failedToCallListenerInDiscovery(t);
         }
      }
   }

   private boolean checkExpiration()
   {
      boolean changed = false;
      long now = System.currentTimeMillis();

      Iterator<Map.Entry<String, DiscoveryEntry>> iter = connectors.entrySet().iterator();

      // Weed out any expired connectors

      while (iter.hasNext())
      {
         Map.Entry<String, DiscoveryEntry> entry = iter.next();

         if (entry.getValue().getLastUpdate() + timeout <= now)
         {
            if (isTrace)
            {
               HornetQLogger.LOGGER.trace("Timed out node on discovery:" + entry.getValue());
            }
            iter.remove();

            changed = true;
         }
      }

      return changed;
   }
}
