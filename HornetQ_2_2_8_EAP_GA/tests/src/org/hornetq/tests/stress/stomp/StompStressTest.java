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
package org.hornetq.tests.stress.stomp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

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

public class StompStressTest extends UnitTestCase
{
   private static final transient Logger log = Logger.getLogger(StompStressTest.class);

   private static final int COUNT = 1000;

   private int port = 61613;

   private Socket stompSocket;

   private ByteArrayOutputStream inputBuffer;

   private String destination = "stomp.stress.queue";

   private HornetQServer server;

   public void testSendAndReceiveMessage() throws Exception
   {
      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + destination + "\n" + "ack:auto\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = "SEND\n" + "destination:" + destination + "\n";

      for (int i = 0; i < COUNT; i++)
      {
         System.out.println(">>> " + i);
         sendFrame(frame + "count:" + i + "\n\n" + Stomp.NULL);
      }

      for (int i = 0; i < COUNT; i++)
      {
         System.out.println("<<< " + i);
         frame = receiveFrame(10000);
         Assert.assertTrue(frame.startsWith("MESSAGE"));
         Assert.assertTrue(frame.indexOf("destination:") > 0);
         Assert.assertTrue(frame.indexOf("count:" + i) > 0);
      }

      frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
      sendFrame(frame);
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
      config.getQueueConfigurations().add(new CoreQueueConfiguration(destination, destination, null, false));
      return HornetQServers.newHornetQServer(config);
   }

   protected void tearDown() throws Exception
   {
      if (stompSocket != null)
      {
         stompSocket.close();
      }
      server.stop();

      super.tearDown();
   }

   protected Socket createSocket() throws IOException
   {
      return new Socket("127.0.0.1", port);
   }

   public void sendFrame(String data) throws Exception
   {
      byte[] bytes = data.getBytes("UTF-8");
      OutputStream outputStream = stompSocket.getOutputStream();
      for (int i = 0; i < bytes.length; i++)
      {
         outputStream.write(bytes[i]);
      }
      outputStream.flush();
   }

   public void sendFrame(byte[] data) throws Exception
   {
      OutputStream outputStream = stompSocket.getOutputStream();
      for (int i = 0; i < data.length; i++)
      {
         outputStream.write(data[i]);
      }
      outputStream.flush();
   }

   public String receiveFrame(long timeOut) throws Exception
   {
      stompSocket.setSoTimeout((int)timeOut);
      InputStream is = stompSocket.getInputStream();
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
