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

package org.hornetq.tests.integration.management;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.QueueControl;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.protocol.stomp.Stomp;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.tests.util.RandomUtil;

/**
 * A ManagementWithStompTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ManagementWithStompTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected HornetQServer server;

   protected ClientSession session;

   private Socket stompSocket;

   private ByteArrayOutputStream inputBuffer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   @Test
   public void testGetManagementAttributeFromStomp() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + queue + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      // retrieve the address of the queue
      frame = "\nSEND\n" + "destination:" + HornetQDefaultConfiguration.getDefaultManagementAddress() + "\n" +
            "reply-to:" + address + "\n" +
            "_HQ_ResourceName:" + ResourceNames.CORE_QUEUE + queue + "\n" +
            "_HQ_Attribute: Address\n\n" +
            Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      System.out.println(frame);
      assertTrue(frame.contains("_HQ_OperationSucceeded:true"));
      // the address will be returned in the message body in a JSON array
      Assert.assertTrue(frame.contains("[\"" + address + "\"]"));

      frame = "UNSUBSCRIBE\n" + "destination:" + queue + "\n" +
         "receipt: 123\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      String disconnectFrame = "DISCONNECT\n\n" + Stomp.NULL;
      sendFrame(disconnectFrame);

      session.deleteQueue(queue);
   }

   @Test
   public void testInvokeOperationFromStomp() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, null, false);

      String frame = "CONNECT\n" + "login: brianm\n" + "passcode: wombats\n\n" + Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      Assert.assertTrue(frame.startsWith("CONNECTED"));

      frame = "SUBSCRIBE\n" + "destination:" + queue + "\n\n" + Stomp.NULL;
      sendFrame(frame);

      // count number of message with filter "color = 'blue'"
      frame = "\nSEND\n" + "destination:" + HornetQDefaultConfiguration.getDefaultManagementAddress() + "\n" +
            "reply-to:" + address + "\n" +
            "_HQ_ResourceName:" + ResourceNames.CORE_QUEUE + queue + "\n" +
            "_HQ_OperationName: countMessages\n\n" +
            "[\"color = 'blue'\"]" +
            Stomp.NULL;
      sendFrame(frame);

      frame = receiveFrame(10000);
      System.out.println(frame);
      assertTrue(frame.contains("_HQ_OperationSucceeded:true"));
      // there is no such messages => 0 returned in a JSON array
      assertTrue(frame.contains("[0]"));

      frame = "UNSUBSCRIBE\n" + "destination:" + queue + "\n" +
         "receipt: 123\n\n" +
         Stomp.NULL;
      sendFrame(frame);
      waitForReceipt();

      String disconnectFrame = "DISCONNECT\n\n" + Stomp.NULL;
      sendFrame(disconnectFrame);

      session.deleteQueue(queue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PROTOCOL_PROP_NAME, ProtocolType.STOMP.toString());
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);
      conf.getAcceptorConfigurations().add(stompTransport);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      server = HornetQServers.newHornetQServer(conf, mbeanServer, false, "brianm", "wombats");

      server.start();

      locator = createInVMNonHALocator();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnNonDurableSend(true);
      ClientSessionFactory sf = createSessionFactory(locator);
      session = sf.createSession(false, true, false);
      session.start();

      stompSocket = new Socket("127.0.0.1", TransportConstants.DEFAULT_STOMP_PORT);
      inputBuffer = new ByteArrayOutputStream();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      session.close();

      server.stop();

      locator.close();

      session = null;

      server = null;

      super.tearDown();
   }

   protected QueueControl createManagementControl(final SimpleString address, final SimpleString queue) throws Exception
   {
      QueueControl queueControl = ManagementControlHelper.createQueueControl(address, queue, mbeanServer);

      return queueControl;
   }

   // Private -------------------------------------------------------

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

   protected void waitForReceipt() throws Exception
   {
      String frame = receiveFrame(50000);
      assertNotNull(frame);
      assertTrue(frame.indexOf("RECEIPT") > -1);
   }

   // Inner classes -------------------------------------------------

}
