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

import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.tests.integration.management.ManagementControlHelper;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A HeuristicXATest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class HeuristicXATest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   final SimpleString ADDRESS = new SimpleString("ADDRESS");

   // Attributes ----------------------------------------------------

   MBeanServer mbeanServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testInvalidCall() throws Exception
   {
      Configuration configuration = createDefaultConfig();
      configuration.setJMXManagementEnabled(true);

      MessagingServer server = createServer(false,
                                               configuration,
                                               mbeanServer,
                                               new HashMap<String, AddressSettings>());

      try
      {
         server.start();

         MessagingServerControlMBean jmxServer = ManagementControlHelper.createMessagingServerControl(mbeanServer);

         assertFalse(jmxServer.commitPreparedTransaction("Nananananana"));
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }

   }

   public void testHerusticCommit() throws Exception
   {
      internalTest(true);
   }

   public void testHerusticRollback() throws Exception
   {
      internalTest(false);
   }

   private void internalTest(final boolean isCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig();
      configuration.setJMXManagementEnabled(true);

      MessagingServer server = createServer(false,
                                               configuration,
                                               mbeanServer,
                                               new HashMap<String, AddressSettings>());
      try
      {
         server.start();
         Xid xid = newXID();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(true, false, false);

         session.createQueue(ADDRESS, ADDRESS, true);

         session.start(xid, XAResource.TMNOFLAGS);

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientMessage msg = session.createClientMessage(true);

         msg.getBody().writeBytes(new byte[123]);

         producer.send(msg);

         session.end(xid, XAResource.TMSUCCESS);

         session.prepare(xid);

         session.close();

         MessagingServerControlMBean jmxServer = ManagementControlHelper.createMessagingServerControl(mbeanServer);

         String preparedTransactions[] = jmxServer.listPreparedTransactions();

         assertEquals(1, preparedTransactions.length);

         System.out.println(preparedTransactions[0]);

         if (isCommit)
         {
            jmxServer.commitPreparedTransaction(XidImpl.toBase64String(xid));
         }
         else
         {
            jmxServer.rollbackPreparedTransaction(XidImpl.toBase64String(xid));
         }

         preparedTransactions = jmxServer.listPreparedTransactions();
         assertEquals(0, preparedTransactions.length);

         if (isCommit)
         {
            assertEquals(1,
                         ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

            session = sf.createSession(false, false, false);

            session.start();
            ClientConsumer consumer = session.createConsumer(ADDRESS);
            msg = consumer.receive(1000);
            assertNotNull(msg);
            msg.acknowledge();
            assertEquals(123, msg.getBodySize());

            session.commit();
            session.close();
         }

         assertEquals(0,
                      ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      MBeanServerFactory.releaseMBeanServer(mbeanServer);
      super.tearDown();
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      mbeanServer = MBeanServerFactory.createMBeanServer();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
