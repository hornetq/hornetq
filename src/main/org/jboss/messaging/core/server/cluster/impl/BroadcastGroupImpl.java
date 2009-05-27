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

package org.jboss.messaging.core.server.cluster.impl;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.UUIDGenerator;

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
   
   //Each broadcast group has a unique id - we use this to detect when more than one group broadcasts the same node id
   //on the network which would be an error
   private final String uniqueID;

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
           
      this.uniqueID = UUIDGenerator.getInstance().generateStringUUID();
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
         socket = new DatagramSocket();
      }

      started = true;
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
      
      MessagingBuffer buff = ChannelBuffers.dynamicBuffer(4096);
     
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
      
      byte[] data = buff.array();
            
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
         log.error("Failed to broadcast connector configs", e);
      }
   }

   public synchronized void setScheduledFuture(final ScheduledFuture<?> future)
   {
      this.future = future;
   }

}
