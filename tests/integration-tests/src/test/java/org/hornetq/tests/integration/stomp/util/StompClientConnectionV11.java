/*
 * Copyright 2010 Red Hat, Inc.
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
package org.hornetq.tests.integration.stomp.util;

import java.io.IOException;

/**
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public class StompClientConnectionV11 extends AbstractStompClientConnection
{
   public static final String STOMP_COMMAND = "STOMP";
   
   public static final String ACCEPT_HEADER = "accept-version";
   public static final String HOST_HEADER = "host";
   public static final String VERSION_HEADER = "version";
   public static final String RECEIPT_HEADER = "receipt";
   
   private Pinger pinger;
   private volatile int serverPingCounter;

   public StompClientConnectionV11(String host, int port) throws IOException
   {
      super("1.1", host, port);
   }

   public void connect(String username, String passcode) throws IOException, InterruptedException
   {
      ClientStompFrame frame = factory.newFrame(CONNECT_COMMAND);
      frame.addHeader(ACCEPT_HEADER, "1.1");
      frame.addHeader(HOST_HEADER, "localhost");
      if (username != null)
      {
         frame.addHeader(LOGIN_HEADER, username);
         frame.addHeader(PASSCODE_HEADER, passcode);
      }

      ClientStompFrame response = this.sendFrame(frame);
      
      if (response.getCommand().equals(CONNECTED_COMMAND))
      {
         String version = response.getHeader(VERSION_HEADER);
         assert(version.equals("1.1"));
         
         this.username = username;
         this.passcode = passcode;
         this.connected = true;
      }
      else
      {
         connected = false;
      }
   }
   
   public void connect(String username, String passcode, String clientID) throws IOException, InterruptedException
   {
      ClientStompFrame frame = factory.newFrame(CONNECT_COMMAND);
      frame.addHeader(ACCEPT_HEADER, "1.1");
      frame.addHeader(HOST_HEADER, "localhost");
      frame.addHeader(CLIENT_ID_HEADER, clientID);
      
      if (username != null)
      {
         frame.addHeader(LOGIN_HEADER, username);
         frame.addHeader(PASSCODE_HEADER, passcode);
      }

      ClientStompFrame response = this.sendFrame(frame);
      
      if (response.getCommand().equals(CONNECTED_COMMAND))
      {
         String version = response.getHeader(VERSION_HEADER);
         assert(version.equals("1.1"));
         
         this.username = username;
         this.passcode = passcode;
         this.connected = true;
      }
      else
      {
         connected = false;
      }
   }

   public void connect1(String username, String passcode) throws IOException, InterruptedException
   {
      ClientStompFrame frame = factory.newFrame(STOMP_COMMAND);
      frame.addHeader(ACCEPT_HEADER, "1.0,1.1");
      frame.addHeader(HOST_HEADER, "127.0.0.1");
      if (username != null)
      {
         frame.addHeader(LOGIN_HEADER, username);
         frame.addHeader(PASSCODE_HEADER, passcode);
      }

      ClientStompFrame response = this.sendFrame(frame);
      
      if (response.getCommand().equals(CONNECTED_COMMAND))
      {
         String version = response.getHeader(VERSION_HEADER);
         assert(version.equals("1.1"));
         
         this.username = username;
         this.passcode = passcode;
         this.connected = true;
      }
      else
      {
         System.out.println("Connection failed with frame " + response);
         connected = false;
      }
   }

   @Override
   public void disconnect() throws IOException, InterruptedException
   {
      stopPinger();
      
      ClientStompFrame frame = factory.newFrame(DISCONNECT_COMMAND);
      frame.addHeader("receipt", "1");
      
      ClientStompFrame result = this.sendFrame(frame);
      
      if (result == null || (!"RECEIPT".equals(result.getCommand())) || (!"1".equals(result.getHeader("receipt-id"))))
      {
         throw new IOException("Disconnect failed! " + result);
      }
      
      close();
      
      connected = false;
   }

   @Override
   public ClientStompFrame createFrame(String command)
   {
      return new ClientStompFrameV11(command);
   }

   @Override
   public void startPinger(long interval)
   {
      pinger = new Pinger(interval);
      pinger.startPing();
   }

   @Override
   public void stopPinger()
   {
      if (pinger != null)
      {
         pinger.stopPing();
         try
         {
            pinger.join();
         }
         catch (InterruptedException e)
         {
            e.printStackTrace();
         }
         pinger = null;
      }
   }
   
   private class Pinger extends Thread
   {
      long pingInterval;
      ClientStompFrameV11 pingFrame;
      volatile boolean stop = false;
      
      public Pinger(long interval)
      {
         this.pingInterval = interval;
         pingFrame = (ClientStompFrameV11) createFrame("STOMP");
         pingFrame.setBody("\n");
         pingFrame.setForceOneway();
         pingFrame.setPing(true);
      }
      
      public void startPing()
      {
         start();
      }
      
      public synchronized void stopPing()
      {
         stop = true;
         this.notify();
      }
      
      public void run()
      {
         synchronized (this)
         {
            while (!stop)
            {
               try
               {
                  sendFrame(pingFrame);
                  
                  this.wait(pingInterval);
               }
               catch (Exception e)
               {
                  stop = true;
                  e.printStackTrace();
               }
            }
         }
      }
   }

   @Override
   public int getServerPingNumber()
   {
      return serverPingCounter;
   }
   
   protected void incrementServerPing()
   {
      serverPingCounter++;
   }


}
