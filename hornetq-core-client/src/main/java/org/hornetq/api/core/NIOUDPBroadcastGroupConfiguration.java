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

package org.hornetq.api.core;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.client.HornetQClientLogger;



/**
 * The configuration used to determine how the server will broadcast members.
 * <p>
 * This is analogous to {@link org.hornetq.api.core.DiscoveryGroupConfiguration}
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Created 18 Nov 2008 08:44:30
 */
public final class NIOUDPBroadcastGroupConfiguration implements BroadcastEndpointFactoryConfiguration, DiscoveryGroupConfigurationCompatibilityHelper
{
   private static final long serialVersionUID = 1052413739064253955L;

   private transient final String localBindAddress;

   private transient final int localBindPort;

   private final String groupAddress;

   private final int groupPort;

   public NIOUDPBroadcastGroupConfiguration(final String groupAddress,
                                            final int groupPort,
                                            final String localBindAddress,
                                            final int localBindPort)
   {
      this.groupAddress = groupAddress;
      this.groupPort = groupPort;
      this.localBindAddress = localBindAddress;
      this.localBindPort = localBindPort;
   }

   public BroadcastEndpointFactory createBroadcastEndpointFactory()
   {
      return new BroadcastEndpointFactory() {
         @Override
         public BroadcastEndpoint createBroadcastEndpoint() throws Exception {
            return new UDPBroadcastEndpoint(groupAddress != null ? InetAddress.getByName(groupAddress) : null,
                  groupPort,
                  localBindAddress != null ? InetAddress.getByName(localBindAddress) : null,
                  localBindPort);
         }
      };
   }

   public String getGroupAddress()
   {
      return groupAddress;
   }

   public int getGroupPort()
   {
      return groupPort;
   }

   public int getLocalBindPort()
   {
      return localBindPort;
   }

   public String getLocalBindAddress()
   {
      return localBindAddress;
   }

   /**
    * <p> This is the member discovery implementation using direct UDP. It was extracted as a refactoring from
    * {@link org.hornetq.core.cluster.DiscoveryGroup}</p>
    * @author Tomohisa
    * @author Howard Gao
    * @author Clebert Suconic
    */
   private static class UDPBroadcastEndpoint implements BroadcastEndpoint
   {
      private static final int SOCKET_TIMEOUT = 500;

      private final InetAddress localAddress;

      private final int localBindPort;

      private final InetAddress groupAddress;

      private final int groupPort;

      private DatagramSocket broadcastingSocket;

      private DatagramChannel receivingSocket;

      private volatile boolean open;

      private Selector selector;

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
         while (open)
         {
            try
            {
               if (selector != null) {
                  selector.select();
               }
               selector.selectedKeys().clear();
               SocketAddress res = receivingSocket.receive(ByteBuffer.wrap(data));
               if (res == null) {
                  continue;
               }
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
                  HornetQClientLogger.LOGGER.warn(this + " getting exception when receiving broadcasting.", e);
               }
            }
            break;
         }
         return data;
      }

      public byte[] receiveBroadcast(long time, TimeUnit unit) throws Exception
      {
         // We just use the regular method on UDP, there's no timeout support
         // and this is basically for tests only
         return receiveBroadcast();
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
               HornetQClientLogger.LOGGER.broadcastGroupBindError();
            }
            broadcastingSocket = new DatagramSocket();
         }

         open = true;
      }

      public void openClient() throws Exception
      {
         selector = Selector.open();
         receivingSocket = DatagramChannel.open(StandardProtocolFamily.INET);
         Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
         while (networkInterfaces.hasMoreElements()){
            receivingSocket.join(groupAddress, networkInterfaces.nextElement());
         }
         receivingSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
         receivingSocket.bind(new InetSocketAddress(groupAddress, groupPort));

         if (selector != null) {
            receivingSocket.configureBlocking(false);
            receivingSocket.register(selector, SelectionKey.OP_READ);
         } else {
            receivingSocket.configureBlocking(true);
         }

         open = true;
      }

      //@Todo: using isBroadcast to share endpoint between broadcast and receiving
      public void close(boolean isBroadcast) throws Exception
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

         if (selector != null) {
            selector.wakeup();
            selector.close();
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

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((groupAddress == null) ? 0 : groupAddress.hashCode());
      result = prime * result + groupPort;
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      NIOUDPBroadcastGroupConfiguration other = (NIOUDPBroadcastGroupConfiguration)obj;
      if (groupAddress == null)
      {
         if (other.groupAddress != null)
            return false;
      }
      else if (!groupAddress.equals(other.groupAddress))
         return false;
      if (groupPort != other.groupPort)
         return false;
      return true;
   }
}
