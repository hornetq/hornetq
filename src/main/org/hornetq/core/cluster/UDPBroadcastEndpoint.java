package org.hornetq.core.cluster;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.util.Map;

import org.hornetq.core.cluster.BroadcastEndpoint;
import org.hornetq.core.logging.Logger;

public class UDPBroadcastEndpoint implements BroadcastEndpoint
{
   private static final Logger log = Logger.getLogger(UDPBroadcastEndpoint.class);

   private static final int SOCKET_TIMEOUT = 500;

   private InetAddress localAddress;

   private int localPort;

   private InetAddress groupAddress;

   private int groupPort;

   private DatagramSocket broadcastingSocket;
   
   private MulticastSocket receivingSocket;
   
   private volatile boolean started;

   public void init(Map<String, Object> params) throws Exception
   {
      String param = (String) params.get("local-bind-address");
      localAddress = null;
      if (param != null)
      {
         localAddress = InetAddress.getByName(param);
      }
      
      param = (String) params.get("local-bind-port");
      localPort = -1;
      if (param != null)
      {
         localPort = Integer.valueOf(param);
      }
      
      param = (String) params.get("group-address");
      groupAddress = null;
      if (param != null)
      {
         groupAddress = InetAddress.getByName(param);
      }
      
      param = (String) params.get("group-port");
      groupPort = -1;
      if (param != null)
      {
         groupPort = Integer.valueOf(param);
      }
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

      while (started)
      {
         try
         {
            receivingSocket.receive(packet);
         }
         catch (InterruptedIOException e)
         {
            continue;
         }
         catch (IOException e)
         {
            if (started)
            {
               log.warn(this + " getting exception when receiving broadcasting.", e);
            }
         }
         break;
      }
      return data;
   }

   public void start(boolean broadcasting) throws Exception
   {
      if (broadcasting)
      {
         if (localPort != -1)
         {
            broadcastingSocket = new DatagramSocket(localPort, localAddress);
         }
         else
         {
            if (localAddress != null)
            {
               log.warn("local-bind-address specified for broadcast group but no local-bind-port specified so socket will NOT be bound to a local address/port");
            }
            broadcastingSocket = new DatagramSocket();
         }
      }
      else
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
               log.warn("Could not bind to " + groupAddress.getHostAddress() + "(" + 
                        (groupAddress instanceof Inet4Address ? "IPv4" : "IPv6") + " address); " +
                        "make sure your discovery group-address is of the same type as the IP stack (IPv4 or IPv6)." +
                        "\nIgnoring discovery group-address, but this may lead to cross talking.");

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
      }
      started = true;
   }

   public void stop() throws Exception
   {
      started = false;
      
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
