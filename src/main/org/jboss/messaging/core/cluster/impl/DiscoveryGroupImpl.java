/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.cluster.impl;

import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.cluster.DiscoveryEntry;
import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.DiscoveryListener;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.Pair;

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

      thread = new Thread(this);

      thread.setDaemon(true);

      thread.start();
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
   
   private Map<String, UniqueIDEntry> uniqueIDMap = new HashMap<String, UniqueIDEntry>();
   
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

            MessagingBuffer buffer = ChannelBuffers.wrappedBuffer(data);

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
