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
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.junit.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A SessionCloseTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class SessionCloseTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCanNotUseAClosedSession() throws Exception
   {

      final ClientSession session = sf.createSession(false, true, true);

      session.close();

      Assert.assertTrue(session.isClosed());

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createProducer();
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createConsumer(RandomUtil.randomSimpleString());
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createQueue(RandomUtil.randomSimpleString(),
                                RandomUtil.randomSimpleString(),
                                RandomUtil.randomBoolean());
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createTemporaryQueue(RandomUtil.randomSimpleString(), RandomUtil.randomSimpleString());
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.start();
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.stop();
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.commit();
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.rollback();
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.queueQuery(RandomUtil.randomSimpleString());
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.bindingQuery(RandomUtil.randomSimpleString());
         }
      });

   }

   @Test
   public void testCanNotUseXAWithClosedSession() throws Exception
   {

      final ClientSession session = sf.createSession(true, false, false);

      session.close();

      Assert.assertTrue(session.isXA());
      Assert.assertTrue(session.isClosed());

      UnitTestCase.expectXAException(XAException.XA_RETRY, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.commit(RandomUtil.randomXid(), RandomUtil.randomBoolean());
         }
      });

      UnitTestCase.expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.end(RandomUtil.randomXid(), XAResource.TMSUCCESS);
         }
      });

      UnitTestCase.expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.forget(RandomUtil.randomXid());
         }
      });

      UnitTestCase.expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.prepare(RandomUtil.randomXid());
         }
      });

      UnitTestCase.expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.recover(XAResource.TMSTARTRSCAN);
         }
      });

      UnitTestCase.expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.rollback(RandomUtil.randomXid());
         }
      });

      UnitTestCase.expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.start(RandomUtil.randomXid(), XAResource.TMNOFLAGS);
         }
      });

   }

   @Test
   public void testCloseHierarchy() throws Exception
   {
      SimpleString address = RandomUtil.randomSimpleString();
      SimpleString queue = RandomUtil.randomSimpleString();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);
      ClientConsumer consumer = session.createConsumer(queue);

      session.close();

      Assert.assertTrue(session.isClosed());
      Assert.assertTrue(producer.isClosed());
      Assert.assertTrue(consumer.isClosed());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig();
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));
      config.setSecurityEnabled(false);
      server = HornetQServers.newHornetQServer(config, false);

      server.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      sf = createSessionFactory(locator);

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (sf != null)
      {
         sf.close();
      }

      if (server != null)
      {
         server.stop();
      }

      sf = null;

      server = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
