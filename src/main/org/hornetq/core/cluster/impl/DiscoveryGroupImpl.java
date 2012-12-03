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

package org.hornetq.core.cluster.impl;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQInterruptedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationService;
import org.hornetq.utils.TypedProperties;

/**
 * A DiscoveryGroupImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 17 Nov 2008 13:21:45
 *
 */
public class DiscoveryGroupImpl implements Runnable, DiscoveryGroup
{
   private static final Logger log = Logger.getLogger(DiscoveryGroupImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();

   private static final int SOCKET_TIMEOUT = 500;

   private MulticastSocket socket;

   private final List<DiscoveryListener> listeners = new ArrayList<DiscoveryListener>();

   private final String name;

   private Thread thread;

   private boolean received;

   private final Object waitLock = new Object();

   private final Map<String, DiscoveryEntry> connectors = new ConcurrentHashMap<String, DiscoveryEntry>();

   private final long timeout;

   private volatile boolean started;

   private final String nodeID;

   private final InetAddress localBindAddress;

   private final InetAddress groupAddress;

   private final int groupPort;

   private final Map<String, String> uniqueIDMap = new HashMap<String, String>();

   private NotificationService notificationService;

   public DiscoveryGroupImpl(final String nodeID,
                             final String name,
                             final InetAddress localBindAddress,
                             final InetAddress groupAddress,
                             final int groupPort,
                             final long timeout) throws Exception
   {
      this.nodeID = nodeID;

      this.name = name;

      this.timeout = timeout;

      this.localBindAddress = localBindAddress;

      this.groupAddress = groupAddress;

      this.groupPort = groupPort;
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

      try
      {

         // HORNETQ-874
         if (checkForLinux() || checkForSolaris() || checkForHp())
         {
            try {
               socket = new MulticastSocket(new InetSocketAddress(groupAddress, groupPort));
            } catch (IOException e) {
               StringBuilder builder = new StringBuilder();
               builder.append("Could not bind to ").append(groupAddress.getHostAddress()).append(" (");
               builder.append((groupAddress instanceof Inet4Address) ? "IPv4" : "IPv6").append(" address)");
               builder.append("; make sure your discovery group-address is of the same type as the IP stack (IPv4 or IPv6).");
               builder.append("\nIgnoring discovery group-address, but this may lead to cross talking.");

               log.warn(builder.toString(), e);

               socket = new MulticastSocket(groupPort);
            }
         }
         else
         {
            socket = new MulticastSocket(groupPort);
         }

         if (localBindAddress != null)
         {
            socket.setInterface(localBindAddress);
         }

         socket.joinGroup(groupAddress);

         socket.setSoTimeout(DiscoveryGroupImpl.SOCKET_TIMEOUT);
      }
      catch (IOException e)
      {
         log.error("Failed to create discovery group socket", e);

         return;
      }

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
         socket.close();

         socket = null;
      }
      catch (Throwable ignored)
      {
         log.warn(ignored.toString(), ignored);
      }

      try
      {
         thread.interrupt();
         thread.join(10000);
         if(thread.isAlive())
         {
            log.warn("Timed out waiting to stop discovery thread");
         }
      }
      catch (InterruptedException e)
      {
         throw new HornetQInterruptedException(e);
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
            DiscoveryGroupImpl.log.warn("unable to send notification when discovery group is stopped", e);
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
               throw new HornetQInterruptedException(e);
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
            log.warn("There are more than one servers on the network broadcasting the same node id. " + "You will see this message exactly once (per node) if a node is restarted, in which case it can be safely "
                     + "ignored. But if it is logged continuously it means you really do have more than one node on the same network "
                     + "active concurrently with the same node id. This could occur if you have a backup node active at the same time as "
                     + "its live node. nodeID=" + originatingNodeID);
            uniqueIDMap.put(originatingNodeID, uniqueID);
         }
      }
   }

   public void run()
   {
      try
      {
         // TODO - can we use a smaller buffer size?
         final byte[] data = new byte[65535];

         while (true)
         {
            if (!started)
            {
               return;
            }

            final DatagramPacket packet = new DatagramPacket(data, data.length);

            try
            {
               socket.receive(packet);
            }
            catch (SocketException e)
            {
               if (!started)
               {
                  return;
               }
               else
               {
                  log.warn(e.getMessage(), e);
               }
            }
            catch (InterruptedIOException e)
            {
               if (!started)
               {
                  return;
               }
               else
               {
                  continue;
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
                  log.trace("Connectors changed on Discovery:");
                  for (DiscoveryEntry connector : connectors.values())
                  {
                     log.trace(connector);
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
         DiscoveryGroupImpl.log.error("Failed to receive datagram", e);
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
            DiscoveryGroupImpl.log.error("Failed to call discovery listener", t);
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
               log.trace("Timed out node on discovery:" + entry.getValue());
            }
            iter.remove();

            changed = true;
         }
      }

      return changed;
   }

   private static boolean checkForLinux() {
      return checkForPresence("os.name", "linux");
   }

   private static boolean checkForHp() {
      return checkForPresence("os.name", "hp");
   }

   private static boolean checkForSolaris() {
      return checkForPresence("os.name", "sun");
   }

   private static boolean checkForPresence(String key, String value) {
      try {
         String tmp=System.getProperty(key);
         return tmp != null && tmp.trim().toLowerCase().startsWith(value);
      }
      catch(Throwable t) {
         return false;
      }
   }

}
