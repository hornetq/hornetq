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

import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomSimpleString;
import static org.hornetq.tests.util.RandomUtil.randomXid;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

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

   public void testCanNotUseAClosedSession() throws Exception
   {

      final ClientSession session = sf.createSession(false, true, true);

      session.close();

      assertTrue(session.isClosed());

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createProducer();
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createConsumer(randomSimpleString());
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createQueue(randomSimpleString(), randomSimpleString(), randomBoolean());
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.createTemporaryQueue(randomSimpleString(), randomSimpleString());
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.start();
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.stop();
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.commit();
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.rollback();
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.queueQuery(randomSimpleString());
         }
      });

      expectHornetQException(HornetQException.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            session.bindingQuery(randomSimpleString());
         }
      });

   }

   public void testCanNotUseXAWithClosedSession() throws Exception
   {

      final ClientSession session = sf.createSession(true, false, false);

      session.close();

      assertTrue(session.isXA());
      assertTrue(session.isClosed());

      expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.commit(randomXid(), randomBoolean());
         }
      });

      expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.end(randomXid(), XAResource.TMSUCCESS);
         }
      });

      expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.forget(randomXid());
         }
      });

      expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.prepare(randomXid());
         }
      });

      expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.recover(XAResource.TMSTARTRSCAN);
         }
      });

      expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.rollback(randomXid());
         }
      });

      expectXAException(XAException.XAER_RMERR, new HornetQAction()
      {
         public void run() throws XAException
         {
            session.start(randomXid(), XAResource.TMNOFLAGS);
         }
      });

   }

   public void testCloseHierarchy() throws Exception
   {
      SimpleString address = randomSimpleString();
      SimpleString queue = randomSimpleString();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(address, queue, false);

      ClientProducer producer = session.createProducer(address);
      ClientConsumer consumer = session.createConsumer(queue);

      session.close();

      assertTrue(session.isClosed());
      assertTrue(producer.isClosed());
      assertTrue(consumer.isClosed());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = new ConfigurationImpl();
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));
      config.setSecurityEnabled(false);
      server = HornetQ.newHornetQServer(config, false);

      server.start();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));

   }

   @Override
   protected void tearDown() throws Exception
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
