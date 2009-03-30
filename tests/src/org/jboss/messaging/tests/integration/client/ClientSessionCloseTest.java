/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomSimpleString;
import static org.jboss.messaging.tests.util.RandomUtil.randomXid;

import java.io.File;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A SessionCloseTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class ClientSessionCloseTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCanNotUseAClosedSession() throws Exception
   {

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      final ClientSession session = sf.createSession(false, true, true);

      session.close();

      assertTrue(session.isClosed());

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.createProducer();
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.createConsumer(randomSimpleString());
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.createFileConsumer(new File("."), randomSimpleString());
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.createQueue(randomSimpleString(), randomSimpleString(), randomBoolean());
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.createTemporaryQueue(randomSimpleString(), randomSimpleString());
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.start();
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.stop();
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.commit();
         }
      });

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.rollback();
         }
      });
      
      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.queueQuery(randomSimpleString());
         }
      });
      
      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            session.bindingQuery(randomSimpleString());
         }
      });

   }
   
   public void testCanNotUseXAWithClosedSession() throws Exception
   {

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      final ClientSession session = sf.createSession(true, false, false);

      session.close();

      assertTrue(session.isXA());
      assertTrue(session.isClosed());

      expectXAException(XAException.XAER_RMERR, new MessagingAction()
      {
         public void run() throws XAException
         {
            session.commit(randomXid(), randomBoolean());
         }
      });
      
      expectXAException(XAException.XAER_RMERR, new MessagingAction()
      {
         public void run() throws XAException
         {
            session.end(randomXid(), XAResource.TMSUCCESS);
         }
      });

      expectXAException(XAException.XAER_RMERR, new MessagingAction()
      {
         public void run() throws XAException
         {
            session.forget(randomXid());
         }
      });

      expectXAException(XAException.XAER_RMERR, new MessagingAction()
      {
         public void run() throws XAException
         {
            session.prepare(randomXid());
         }
      });

      expectXAException(XAException.XAER_RMERR, new MessagingAction()
      {
         public void run() throws XAException
         {
            session.recover(XAResource.TMSTARTRSCAN);
         }
      });

      expectXAException(XAException.XAER_RMERR, new MessagingAction()
      {
         public void run() throws XAException
         {
            session.rollback(randomXid());
         }
      });

      expectXAException(XAException.XAER_RMERR, new MessagingAction()
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

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
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
      config.setSecurityEnabled(false);
      server = Messaging.newNullStorageMessagingServer(config);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
