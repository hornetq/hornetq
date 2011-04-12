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
package org.hornetq.tests.concurrent.stomp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.stomp.Stomp;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.tests.util.UnitTestCase;

public class ConcurrentStompTest extends UnitTestCase
{
   private static final transient Logger log = Logger.getLogger(ConcurrentStompTest.class);

   private int port = 61613;

   private Socket stompSocket;

   private ByteArrayOutputStream inputBuffer;

   private Socket stompSocket_2;

   private ByteArrayOutputStream inputBuffer_2;

   private HornetQServer server;

   /**
    * Send messages on 1 socket and receives them concurrently on another socket.
    */
   public void testSendManyMessages() throws Exception
   {
      String connect = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;

      sendFrame(stompSocket, connect);
      String connected = receiveFrame(stompSocket, inputBuffer, 10000);
      Assert.assertTrue(connected.startsWith("CONNECTED"));
      
      sendFrame(stompSocket_2, connect);
      connected = receiveFrame(stompSocket_2, inputBuffer_2, 10000);
      Assert.assertTrue(connected.startsWith("CONNECTED"));
      
      final int count = 1000;
      final CountDownLatch latch = new CountDownLatch(count);
      
      String subscribe = 
         "SUBSCRIBE\n" +
         "destination:" + getQueueName() + "\n" +
         "ack:auto\n\n" +
         Stomp.NULL;
      sendFrame(stompSocket_2, subscribe);
      Thread.sleep(2000);
      
      new Thread()
      {
         public void run()
         {
            int i = 0;
            while (true)
            {
               try
               {
                  String frame = receiveFrame(stompSocket_2, inputBuffer_2, 10000);
                  Assert.assertTrue(frame.startsWith("MESSAGE"));
                  Assert.assertTrue(frame.indexOf("destination:") > 0);
                  System.out.println("<<< " + i++);
                  latch.countDown();
               }
               catch (Exception e)
               {
                  break;
               }
            }
         };
      }.start();

      String send = "SEND\n" + "destination:" + getQueueName() + "\n";
      for (int i = 1; i <= count; i++)
      {
         // Thread.sleep(1);
         System.out.println(">>> " + i);
         sendFrame(stompSocket, send + "count:" + i + "\n\n" + Stomp.NULL);
      }

      assertTrue(latch.await(60, TimeUnit.SECONDS));

   }

   // Implementation methods
   // -------------------------------------------------------------------------
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer();
      server.start();

      stompSocket = createSocket();
      inputBuffer = new ByteArrayOutputStream();
      stompSocket_2 = createSocket();
      inputBuffer_2 = new ByteArrayOutputStream();

   }

   private HornetQServer createServer() throws Exception
   {
      Configuration config = createBasicConfig();
      config.setSecurityEnabled(false);
      config.setPersistenceEnabled(false);

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PROTOCOL_PROP_NAME, ProtocolType.STOMP.toString());
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);
      config.getAcceptorConfigurations().add(stompTransport);
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      config.getQueueConfigurations().add(new CoreQueueConfiguration(getQueueName(), getQueueName(), null, false));
      return HornetQServers.newHornetQServer(config);
   }

   protected void tearDown() throws Exception
   {
      if (stompSocket != null)
      {
         stompSocket.close();
      }

      if (stompSocket_2 != null)
      {
         stompSocket_2.close();
      }
      server.stop();

      super.tearDown();
   }

   protected Socket createSocket() throws IOException
   {
      return new Socket("127.0.0.1", port);
   }

   protected String getQueueName()
   {
      return "test";
   }

   public void sendFrame(Socket socket, String data) throws Exception
   {
      byte[] bytes = data.getBytes("UTF-8");
      OutputStream outputStream = socket.getOutputStream();
      for (int i = 0; i < bytes.length; i++)
      {
         outputStream.write(bytes[i]);
      }
      outputStream.flush();
   }

   public String receiveFrame(Socket socket, ByteArrayOutputStream inputBuffer, long timeOut) throws Exception
   {
      socket.setSoTimeout((int)timeOut);
      InputStream is = socket.getInputStream();
      int c = 0;
      for (;;)
      {
         c = is.read();
         if (c < 0)
         {
            throw new IOException("socket closed.");
         }
         else if (c == 0)
         {
            c = is.read();
            if (c != '\n')
            {
               byte[] ba = inputBuffer.toByteArray();
               System.out.println(new String(ba, "UTF-8"));
            }
            Assert.assertEquals("Expecting stomp frame to terminate with \0\n", c, '\n');
            byte[] ba = inputBuffer.toByteArray();
            inputBuffer.reset();
            return new String(ba, "UTF-8");
         }
         else
         {
            inputBuffer.write(c);
         }
      }
   }
}
