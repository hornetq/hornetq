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

import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.Notification;
import org.hornetq.core.management.NotificationService;
import org.hornetq.core.management.NotificationType;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
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

   private static final int SOCKET_TIMEOUT = 500;

   private MulticastSocket socket;

   private final List<DiscoveryListener> listeners = new ArrayList<DiscoveryListener>();

   private final String name;

   private Thread thread;

   private boolean received;

   private final Object waitLock = new Object();

   private final Map<String, DiscoveryEntry> connectors = new HashMap<String, DiscoveryEntry>();

   private final long timeout;

   private volatile boolean started;

   private final String nodeID;

   private final InetAddress groupAddress;

   private final int groupPort;
   
   private Map<String, UniqueIDEntry> uniqueIDMap = new HashMap<String, UniqueIDEntry>();

   private NotificationService notificationService;
   
   public DiscoveryGroupImpl(final String nodeID,
                             final String name,
                             final InetAddress groupAddress,
                             final int groupPort,
                             final long timeout) throws Exception
   {
      this.nodeID = nodeID;

      this.name = name;

      this.timeout = timeout;

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

      socket = new MulticastSocket(groupPort);

      socket.joinGroup(groupAddress);

      socket.setSoTimeout(SOCKET_TIMEOUT);

      started = true;

      thread = new Thread(this, "hornetq-discovery-group-thread-" + name);

      thread.setDaemon(true);

      thread.start();
      
      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, NotificationType.DISCOVERY_GROUP_STARTED, props);
         notificationService.sendNotification(notification );
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

      try
      {
         thread.join();
      }
      catch (InterruptedException e)
      {
      }

      socket.close();

      socket = null;

      thread = null;
      
      if (notificationService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), new SimpleString(name));
         Notification notification = new Notification(nodeID, NotificationType.DISCOVERY_GROUP_STOPPED, props );
         try
         {
            notificationService.sendNotification(notification );
         }
         catch (Exception e)
         {
            log.warn("unable to send notification when discovery group is stopped", e);
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

   public synchronized Map<String, DiscoveryEntry> getDiscoveryEntryMap()
   {
      return new HashMap<String, DiscoveryEntry>(connectors);
   }

   public boolean waitForBroadcast(final long timeout)
   {
      synchronized (waitLock)
      {
         long start = System.currentTimeMillis();

         long toWait = timeout;

         while (!received && toWait > 0)
         {
            try
            {
               waitLock.wait(toWait);
            }
            catch (InterruptedException e)
            {
            }

            long now = System.currentTimeMillis();

            toWait -= now - start;

            start = now;
         }

         boolean ret = received;

         received = false;

         return ret;
      }
   }
   
   private static class UniqueIDEntry
   {
      String uniqueID;
      
      boolean changed;
      
      UniqueIDEntry(final String uniqueID)
      {
         this.uniqueID = uniqueID;
      }
      
      boolean isChanged()
      {
         return changed;
      }
      
      void setChanged()
      {
         changed = true;
      }
      
      String getUniqueID()
      {
         return uniqueID;
      }
      
      void setUniqueID(final String uniqueID)
      {
         this.uniqueID = uniqueID;
      }      
   }
   
   /*
    * This is a sanity check to catch any cases where two different nodes are broadcasting the same node id either
    * due to misconfiguration or problems in failover
    */
   private boolean uniqueIDOK(final String originatingNodeID, final String uniqueID)
   {
      UniqueIDEntry entry = uniqueIDMap.get(originatingNodeID);
      
      if (entry == null)
      {
         entry = new UniqueIDEntry(uniqueID);
         
         uniqueIDMap.put(originatingNodeID, entry);
         
         return true;
      }
      else
      {
         if (entry.getUniqueID().equals(uniqueID))
         {
            return true;
         }
         else
         {
            //We allow one change - this might occur if one node fails over onto its backup which
            //has same node id but different unique id
            if (!entry.isChanged())
            {
               entry.setChanged();
               
               entry.setUniqueID(uniqueID);
               
               return true;
            }
            else
            {
               return false;
            }
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
            
            if (!uniqueIDOK(originatingNodeID, uniqueID))
            {
               log.warn("There seem to be more than one broadcasters on the network broadcasting the same node id");
               
               continue;
            }

            if (nodeID.equals(originatingNodeID))
            {
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

                  boolean existsBackup = buffer.readBoolean();

                  TransportConfiguration backupConnector = null;

                  if (existsBackup)
                  {
                     backupConnector = new TransportConfiguration();

                     backupConnector.decode(buffer);
                  }

                  Pair<TransportConfiguration, TransportConfiguration> connectorPair = new Pair<TransportConfiguration, TransportConfiguration>(connector,
                                                                                                                                                backupConnector);

                  DiscoveryEntry entry = new DiscoveryEntry(connectorPair, System.currentTimeMillis());

                  DiscoveryEntry oldVal = connectors.put(originatingNodeID, entry);

                  if (oldVal == null)
                  {
                     changed = true;
                  }
               }

               long now = System.currentTimeMillis();

               Iterator<Map.Entry<String, DiscoveryEntry>> iter = connectors.entrySet().iterator();
               
               // Weed out any expired connectors

               while (iter.hasNext())
               {
                  Map.Entry<String, DiscoveryEntry> entry = iter.next();

                  if (entry.getValue().getLastUpdate() + timeout <= now)
                  {                
                     iter.remove();

                     changed = true;
                  }
               }
            }

            if (changed)
            {
               callListeners();
            }

            synchronized (waitLock)
            {
               received = true;

               waitLock.notify();
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to receive datagram", e);
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
            log.error("Failed to call discovery listener", t);
         }
      }
   }

}
