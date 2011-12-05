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

package org.hornetq.tests.integration.client;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.client.impl.ClientConsumerInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.LargeServerMessageImpl;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendContinuationMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.server.impl.ServerSessionImpl;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.largemessage.LargeMessageTestBase;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A LargeMessageTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 29-Sep-08 4:04:10 PM
 *
 *
 */
public class InterruptedLargeMessageTest extends LargeMessageTestBase
{
   // Constants -----------------------------------------------------

   final static int RECEIVE_WAIT_TIME = 60000;

   private final int LARGE_MESSAGE_SIZE = HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 3;

   // Attributes ----------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Static --------------------------------------------------------
   private final Logger log = Logger.getLogger(LargeMessageTest.class);

   protected ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      LargeMessageTestInterceptorIgnoreLastPacket.interruptMessages = true;
      clearData();
      locator = createFactory(isNetty());
   }

   protected boolean isNetty()
   {
      return false;
   }

   public void testInterruptLargeMessageSend() throws Exception
   {
      final int messageSize = 3 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.interruptMessages = true;

      try
      {
         HornetQServer server = createServer(true, isNetty());

         server.getConfiguration()
               .getInterceptorClassNames()
               .add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

         server.start();

         locator.setBlockOnNonDurableSend(false);
         locator.setBlockOnDurableSend(false);

         ClientSessionFactory sf = locator.createSessionFactory();

         session = sf.createSession(false, true, true);

         session.createQueue(LargeMessageTest.ADDRESS, LargeMessageTest.ADDRESS, true);

         ClientProducer producer = session.createProducer(LargeMessageTest.ADDRESS);

         Message clientFile = createLargeClientMessage(session, messageSize, true);

         clientFile.setExpiration(System.currentTimeMillis());

         producer.send(clientFile);

         Thread.sleep(500);

         for (ServerSession srvSession : server.getSessions())
         {
            ((ServerSessionImpl)srvSession).clearLargeMessage();
         }

         server.stop(false);
         server.start();

         server.stop();

         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testSendNonPersistentQueue() throws Exception
   {
      final int messageSize = 3 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.interruptMessages = false;

      try
      {
         HornetQServer server = createServer(true, isNetty());

         // server.getConfiguration()
         // .getInterceptorClassNames()
         // .add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

         server.start();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         session = sf.createSession(false, true, true);

         session.createQueue(LargeMessageTest.ADDRESS, LargeMessageTest.ADDRESS, false);

         ClientProducer producer = session.createProducer(LargeMessageTest.ADDRESS);

         for (int i = 0; i < 10; i++)
         {
            Message clientFile = createLargeClientMessage(session, messageSize, true);

            producer.send(clientFile);
         }
         session.commit();

         session.close();

         session = sf.createSession(false, false);

         ClientConsumer cons = session.createConsumer(LargeMessageTest.ADDRESS);

         session.start();

         for (int h = 0; h < 5; h++)
         {
            for (int i = 0; i < 10; i++)
            {
               ClientMessage clientMessage = cons.receive(5000);
               assertNotNull(clientMessage);
               for (int countByte = 0; countByte < messageSize; countByte++)
               {
                  assertEquals(getSamplebyte(countByte), clientMessage.getBodyBuffer().readByte());
               }
               clientMessage.acknowledge();
            }
            session.rollback();
         }

         server.stop(false);
         server.start();

         server.stop();

         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testSendPaging() throws Exception
   {
      final int messageSize = 3 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.interruptMessages = false;

      try
      {
         HornetQServer server = createServer(true, createDefaultConfig(isNetty()), 10000, 20000, new HashMap<String, AddressSettings>());
         
         // server.getConfiguration()
         // .getInterceptorClassNames()
         // .add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

         server.start();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         session = sf.createSession(false, true, true);

         session.createQueue(LargeMessageTest.ADDRESS, LargeMessageTest.ADDRESS, true);
         
         server.getPagingManager().getPageStore(ADDRESS).startPaging();

         ClientProducer producer = session.createProducer(LargeMessageTest.ADDRESS);

         for (int i = 0; i < 10; i++)
         {
            Message clientFile = createLargeClientMessage(session, messageSize, true);

            producer.send(clientFile);
         }
         session.commit();
         
         validateNoFilesOnLargeDir(10);

         for (int h = 0; h < 5; h++)
         {
            session.close();
            
            sf.close();
            
            server.stop();
            
            server.start();
            
            sf = locator.createSessionFactory();

            session = sf.createSession(false, false);

            ClientConsumer cons = session.createConsumer(LargeMessageTest.ADDRESS);

            session.start();

            for (int i = 0; i < 10; i++)
            {
               ClientMessage clientMessage = cons.receive(5000);
               assertNotNull(clientMessage);
               for (int countByte = 0; countByte < messageSize; countByte++)
               {
                  assertEquals(getSamplebyte(countByte), clientMessage.getBodyBuffer().readByte());
               }
               clientMessage.acknowledge();
            }
            if (h == 4)
            {
               session.commit();
            }
            else
            {
               session.rollback();
            }
            
            session.close();
            sf.close();
         }
         
         server.stop(false);
         server.start();

         server.stop();

         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testSendPreparedXA() throws Exception
   {
      final int messageSize = 3 * HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      ClientSession session = null;

      LargeMessageTestInterceptorIgnoreLastPacket.interruptMessages = false;

      try
      {
         HornetQServer server = createServer(true, createDefaultConfig(isNetty()), 10000, 20000, new HashMap<String, AddressSettings>());
         
         // server.getConfiguration()
         // .getInterceptorClassNames()
         // .add(LargeMessageTestInterceptorIgnoreLastPacket.class.getName());

         server.start();

         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnDurableSend(true);

         ClientSessionFactory sf = locator.createSessionFactory();

         session = sf.createSession(true, false, false);
         
         Xid xid1 = newXID();
         Xid xid2 = newXID();

         session.createQueue(LargeMessageTest.ADDRESS, LargeMessageTest.ADDRESS, true);

         ClientProducer producer = session.createProducer(LargeMessageTest.ADDRESS);

         session.start(xid1, XAResource.TMNOFLAGS);

         for (int i = 0; i < 10; i++)
         {
            Message clientFile = createLargeClientMessage(session, messageSize, true);
            clientFile.putIntProperty("txid", 1);
            producer.send(clientFile);
         }
         session.end(xid1, XAResource.TMSUCCESS);
         
         session.prepare(xid1);


         session.start(xid2, XAResource.TMNOFLAGS);


         for (int i = 0; i < 10; i++)
         {
            Message clientFile = createLargeClientMessage(session, messageSize, true);
            clientFile.putIntProperty("txid", 2);
            clientFile.putIntProperty("i", i);
            producer.send(clientFile);
         }
         session.end(xid2, XAResource.TMSUCCESS);
         
         session.prepare(xid2);
         
         session.close();
         sf.close();
         
         server.stop(false);
         server.start();
         
         for (int start = 0 ; start < 2; start++)
         {
            System.out.println("Start " + start);
            
            sf = locator.createSessionFactory();
            
            if (start == 0)
            {
               session = sf.createSession(true, false, false);
               session.commit(xid1, false);
               session.close();
            }
            
            session = sf.createSession(false, false, false);
            ClientConsumer cons1 = session.createConsumer(ADDRESS);
            session.start();
            for (int i = 0 ; i < 10; i++)
            {
               log.info("I = " + i);
               ClientMessage msg = cons1.receive(5000);
               assertNotNull(msg);
               assertEquals(1, msg.getIntProperty("txid").intValue());
               msg.acknowledge();
            }
            
            if (start == 1)
            {
               session.commit();
            }
            else
            {
               session.rollback();
            }
            
            session.close();
            sf.close();
   
            server.stop();
            server.start();
         }
         server.stop();
         
         validateNoFilesOnLargeDir(10);
         
         server.start();

         sf = locator.createSessionFactory();
         
         session = sf.createSession(true, false, false);
         session.rollback(xid2);
         
         sf.close();
         
         server.stop();
         server.start();
         server.stop();

         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public static class LargeMessageTestInterceptorIgnoreLastPacket implements Interceptor
   {

      public static boolean interruptMessages = false;

      /* (non-Javadoc)
       * @see org.hornetq.api.core.Interceptor#intercept(org.hornetq.core.protocol.core.Packet, org.hornetq.spi.core.protocol.RemotingConnection)
       */
      public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
      {
         if (packet instanceof SessionSendContinuationMessage)
         {
            SessionSendContinuationMessage msg = (SessionSendContinuationMessage)packet;
            if (!msg.isContinues() && interruptMessages)
            {
               System.out.println("Ignored a message");
               return false;
            }
         }
         return true;
      }

      // Constants -----------------------------------------------------

      // Attributes ----------------------------------------------------

      // Static --------------------------------------------------------

      // Constructors --------------------------------------------------

      // Public --------------------------------------------------------

      // Package protected ---------------------------------------------

      // Protected -----------------------------------------------------

      // Private -------------------------------------------------------

      // Inner classes -------------------------------------------------

   }

}
