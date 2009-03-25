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
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientFileMessage;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientFileMessageImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientFileMessageTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public void testConsumeFileMessage() throws Exception
   {
      String testDir = System.getProperty("java.io.tmpdir", "/tmp") + "/jbm-unit-test";

      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setMinLargeMessageSize(1000);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession recSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         File directory = new File(testDir);
         directory.mkdirs();
         ClientConsumer cc = recSession.createFileConsumer(directory, queueA);
         recSession.start();
         ClientMessage message = recSession.createClientMessage(false);
         byte[] bytes = new byte[3000];
         message.getBody().writeBytes(bytes);
         cp.send(message);
         ClientFileMessageImpl m = (ClientFileMessageImpl) cc.receive(5000);
         assertNotNull(m);
         FileChannel channel = m.getChannel();
         ByteBuffer dst = ByteBuffer.allocate(3000);
         channel.read(dst);
         assertEqualsByteArrays(bytes, dst.array());
         sendSession.close();
         recSession.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testProduceFileMessage() throws Exception
   {
      String testDir = System.getProperty("java.io.tmpdir", "/tmp") + "/jbm-unit-test";

      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setMinLargeMessageSize(1000);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession recSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         File directory = new File(testDir);
         directory.delete();
         directory.mkdirs();
         ClientConsumer cc = recSession.createConsumer(queueA);
         recSession.start();
         ClientFileMessage message = sendSession.createFileMessage(false);
         byte[] bytes = new byte[3000];
         File src = new File(directory, "test.jbm");
         src.createNewFile();
         FileOutputStream fos = new FileOutputStream(src);
         fos.write(bytes);
         fos.close();
         message.setFile(src);
         cp.send(message);
         ClientMessage m = cc.receive(5000);
         assertNotNull(m);
         byte[] recBytes = new byte[3000];
         m.getBody().readBytes(recBytes);
         assertEqualsByteArrays(bytes, recBytes);
         sendSession.close();
         recSession.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }
}
