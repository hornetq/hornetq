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
package org.hornetq.stomp.tests.util.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public abstract class AbstractStompClientConnection implements StompClientConnection
{
   protected static final String CONNECT_COMMAND = "CONNECT";
   protected static final String CONNECTED_COMMAND = "CONNECTED";
   protected static final String DISCONNECT_COMMAND = "DISCONNECT";
   
   protected static final String LOGIN_HEADER = "login";
   protected static final String PASSCODE_HEADER = "passcode";
   
   
   protected String version;
   protected String host;
   protected int port;
   protected String username;
   protected String passcode;
   protected StompFrameFactory factory;
   protected SocketChannel socketChannel;
   protected ByteBuffer readBuffer;
   
   protected List<Byte> receiveList;
   
   protected BlockingQueue<ClientStompFrame> frameQueue = new LinkedBlockingQueue<ClientStompFrame>();
   
   protected boolean connected = false;

   public AbstractStompClientConnection(String version, String host, int port) throws IOException
   {
      this.version = version;
      this.host = host;
      this.port = port;
      this.factory = StompFrameFactoryFactory.getFactory(version);
      
      initSocket();
   }

   private void initSocket() throws IOException
   {
      socketChannel = SocketChannel.open();
      socketChannel.configureBlocking(true);
      InetSocketAddress remoteAddr = new InetSocketAddress(host, port);
      socketChannel.connect(remoteAddr);
      
      startReaderThread();
   }
   
   private void startReaderThread()
   {
      readBuffer = ByteBuffer.allocateDirect(10240);
      receiveList = new ArrayList<Byte>(10240);
      
      new ReaderThread().start();
   }
   
   public ClientStompFrame sendFrame(ClientStompFrame frame) throws IOException, InterruptedException
   {
      ClientStompFrame response = null;
      ByteBuffer buffer = frame.toByteBuffer();
      while (buffer.remaining() > 0)
      {
         socketChannel.write(buffer);
      }
      
      //now response
      if (frame.needsReply())
      {
         response = receiveFrame();
      }
      return response;
   }
   
   public ClientStompFrame receiveFrame() throws InterruptedException
   {
      return frameQueue.poll(10, TimeUnit.SECONDS);
   }
   
   //put bytes to byte array.
   private void receiveBytes(int n) throws UnsupportedEncodingException
   {
      readBuffer.rewind();
      for (int i = 0; i < n; i++)
      {
         byte b = readBuffer.get();
         if (b == 0)
         {
            //a new frame got.
            int sz = receiveList.size();
            if (sz > 0)
            {
               byte[] frameBytes = new byte[sz];
               for (int j = 0; j < sz; j++)
               {
                  frameBytes[j] = receiveList.get(j);
               }
               ClientStompFrame frame = factory.createFrame(new String(frameBytes, "UTF-8"));
               frameQueue.offer(frame);
               
               receiveList.clear();
            }
         }
         else
         {
            System.out.println("Added to list: " + b);
            receiveList.add(b);
         }
      }
      //clear readbuffer
      readBuffer.rewind();
   }
   
   protected void close() throws IOException
   {
      socketChannel.close();
   }
   
   private class ReaderThread extends Thread
   {
      public void run()
      {
         try
         {
            int n = socketChannel.read(readBuffer);
            
            while (n >= 0)
            {
               System.out.println("read " + n);
               if (n > 0)
               {
                  receiveBytes(n);
               }
               n = socketChannel.read(readBuffer);
            }
            //peer closed
            close();
         
         } 
         catch (IOException e)
         {
            try
            {
               close();
            } 
            catch (IOException e1)
            {
               //ignore
            }
         }
      }
   }

   public void connect() throws Exception
   {
      connect(null, null);
   }
   
   public void connect(String username, String password) throws Exception
   {
      throw new RuntimeException("connect method not implemented!");
   }

}
