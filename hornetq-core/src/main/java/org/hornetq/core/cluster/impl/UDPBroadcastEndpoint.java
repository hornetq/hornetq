/*
 * Copyright 2012 Red Hat, Inc.
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
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;

import org.hornetq.core.cluster.BroadcastEndpoint;
import org.hornetq.core.server.HornetQLogger;

/**
 * <p> This is the member discovery implementation using direct UDP. It was extracted as a refactoring from
 * {@link org.hornetq.core.cluster.DiscoveryGroup}</p>
 * @author Tomohisa
 * @author Howard Gao
 * @author Clebert Suconic
 */
public class UDPBroadcastEndpoint implements BroadcastEndpoint
{
   private static final int SOCKET_TIMEOUT = 500;

   private final InetAddress localAddress;

   private final int localBindPort;

   private final InetAddress groupAddress;

   private final int groupPort;

   private DatagramSocket broadcastingSocket;
   
   private MulticastSocket receivingSocket;
   
   private volatile boolean open;

   public UDPBroadcastEndpoint(final InetAddress groupAddress,
                               final int groupPort,
                               final InetAddress localBindAddress,
                               final int localBindPort) throws UnknownHostException
   {
      this.groupAddress = groupAddress;
      this.groupPort = groupPort;
      this.localAddress = localBindAddress;
      this.localBindPort = localBindPort;
   }


   public void broadcast(byte[] data) throws Exception
   {
      DatagramPacket packet = new DatagramPacket(data, data.length, groupAddress, groupPort);
      broadcastingSocket.send(packet);
   }

   public byte[] receiveBroadcast() throws Exception
   {
      final byte[] data = new byte[65535];
      final DatagramPacket packet = new DatagramPacket(data, data.length);

      while (open)
      {
         try
         {
            receivingSocket.receive(packet);
         }
         // TODO: Do we need this?
         catch (InterruptedIOException e)
         {
            continue;
         }
         catch (IOException e)
         {
            if (open)
            {
               HornetQLogger.LOGGER.warn(this + " getting exception when receiving broadcasting.", e);
            }
         }
         break;
      }
      return data;
   }

   public void openBroadcaster() throws Exception
   {
      if (localBindPort != -1)
      {
         broadcastingSocket = new DatagramSocket(localBindPort, localAddress);
      }
      else
      {
         if (localAddress != null)
         {
            HornetQLogger.LOGGER.broadcastGroupBindError();
         }
         broadcastingSocket = new DatagramSocket();
      }

      open = true;
   }

   public void openClient() throws Exception
   {
      // HORNETQ-874
      if (checkForLinux() || checkForSolaris() || checkForHp())
      {
         try
         {
            receivingSocket = new MulticastSocket(new InetSocketAddress(groupAddress, groupPort));
         }
         catch (IOException e)
         {
            HornetQLogger.LOGGER.ioDiscoveryError(groupAddress.getHostAddress(), groupAddress instanceof Inet4Address ? "IPv4" : "IPv6");

            receivingSocket = new MulticastSocket(groupPort);
         }
      }
      else
      {
         receivingSocket = new MulticastSocket(groupPort);
      }

      if (localAddress != null)
      {
         receivingSocket.setInterface(localAddress);
      }

      receivingSocket.joinGroup(groupAddress);

      receivingSocket.setSoTimeout(SOCKET_TIMEOUT);

      open = true;
   }

   public void close() throws Exception
   {
      open = false;
      
      if (broadcastingSocket != null)
      {
         broadcastingSocket.close();
      }

      if (receivingSocket != null)
      {
         receivingSocket.close();
      }
   }

   private static boolean checkForLinux()
   {
      return checkForPresence("os.name", "linux");
   }

   private static boolean checkForHp()
   {
      return checkForPresence("os.name", "hp");
   }

   private static boolean checkForSolaris()
   {
      return checkForPresence("os.name", "sun");
   }

   private static boolean checkForPresence(String key, String value)
   {
      try
      {
         String tmp=System.getProperty(key);
         return tmp != null && tmp.trim().toLowerCase().startsWith(value);
      }
      catch(Throwable t)
      {
         return false;
      }
   }

}
