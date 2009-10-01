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

import java.util.HashMap;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

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
   
   private static final Logger log = Logger.getLogger(HeuristicXATest.class);


   final SimpleString ADDRESS = new SimpleString("ADDRESS");

   // Attributes ----------------------------------------------------

   private MBeanServer mbeanServer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testInvalidCall() throws Exception
   {
      Configuration configuration = createDefaultConfig();
      configuration.setJMXManagementEnabled(true);

      HornetQServer server = createServer(false, configuration, mbeanServer, new HashMap<String, AddressSettings>());

      try
      {
         server.start();

         HornetQServerControl jmxServer = ManagementControlHelper.createHornetQServerControl(mbeanServer);

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

   public void testHeuristicCommit() throws Exception
   {
      internalTest(true);
   }

   public void testHeuristicRollback() throws Exception
   {
      internalTest(false);
   }

   private void internalTest(final boolean isCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig();
      configuration.setJMXManagementEnabled(true);

      HornetQServer server = createServer(false, configuration, mbeanServer, new HashMap<String, AddressSettings>());
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

         HornetQServerControl jmxServer = ManagementControlHelper.createHornetQServerControl(mbeanServer);

         String preparedTransactions[] = jmxServer.listPreparedTransactions();

         assertEquals(1, preparedTransactions.length);

         System.out.println(preparedTransactions[0]);

         assertEquals(0, jmxServer.listHeuristicCommittedTransactions().length);
         assertEquals(0, jmxServer.listHeuristicRolledBackTransactions().length);

         if (isCommit)
         {
            jmxServer.commitPreparedTransaction(XidImpl.toBase64String(xid));
         }
         else
         {
            jmxServer.rollbackPreparedTransaction(XidImpl.toBase64String(xid));
         }

         assertEquals(0, jmxServer.listPreparedTransactions().length);
         if (isCommit)
         {
            assertEquals(1, jmxServer.listHeuristicCommittedTransactions().length);
            assertEquals(0, jmxServer.listHeuristicRolledBackTransactions().length);
         }
         else
         {
            assertEquals(0, jmxServer.listHeuristicCommittedTransactions().length);
            assertEquals(1, jmxServer.listHeuristicRolledBackTransactions().length);
         }

         if (isCommit)
         {
            assertEquals(1, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

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

         assertEquals(0, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

      }
      finally
      {
         if (server.isStarted())
         {            
            server.stop();
         }
      }
      
      

   }
   
   public void testHeuristicCommitWithRestart() throws Exception
   {
      doHeuristicCompletionWithRestart(true);
   }
   
   public void testHeuristicRollbackWithRestart() throws Exception
   {
      doHeuristicCompletionWithRestart(false);
   }

   private void doHeuristicCompletionWithRestart(final boolean isCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig();
      configuration.setJMXManagementEnabled(true);

      HornetQServer server = createServer(true, configuration, mbeanServer, new HashMap<String, AddressSettings>());
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

         HornetQServerControl jmxServer = ManagementControlHelper.createHornetQServerControl(mbeanServer);

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
            assertEquals(1, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

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

         assertEquals(0, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

         server.stop();
         
         server.start();

         jmxServer = ManagementControlHelper.createHornetQServerControl(mbeanServer);
         if (isCommit)
         {
            String[] listHeuristicCommittedTransactions = jmxServer.listHeuristicCommittedTransactions();
            assertEquals(1, listHeuristicCommittedTransactions.length);
            System.out.println(listHeuristicCommittedTransactions[0]);
         } else
         {
            String[] listHeuristicRolledBackTransactions = jmxServer.listHeuristicRolledBackTransactions();
            assertEquals(1, listHeuristicRolledBackTransactions.length);
            System.out.println(listHeuristicRolledBackTransactions[0]);
         }
      }
      finally
      {
         if (server.isStarted())
         {            
            server.stop();
         }
      }
   }
   
   public void testRecoverHeuristicCommitWithRestart() throws Exception
   {
      doRecoverHeuristicCompletedTxWithRestart(true);
   }
   
   public void testRecoverHeuristicRollbackWithRestart() throws Exception
   {
      doRecoverHeuristicCompletedTxWithRestart(false);
   }

   private void doRecoverHeuristicCompletedTxWithRestart(boolean heuristicCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig();
      configuration.setJMXManagementEnabled(true);

      HornetQServer server = createServer(true, configuration, mbeanServer, new HashMap<String, AddressSettings>());
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

         HornetQServerControl jmxServer = ManagementControlHelper.createHornetQServerControl(mbeanServer);

         String preparedTransactions[] = jmxServer.listPreparedTransactions();

         assertEquals(1, preparedTransactions.length);
         System.out.println(preparedTransactions[0]);

         if (heuristicCommit)
         {
            jmxServer.commitPreparedTransaction(XidImpl.toBase64String(xid));
         }
         else
         {
            jmxServer.rollbackPreparedTransaction(XidImpl.toBase64String(xid));
         }

         preparedTransactions = jmxServer.listPreparedTransactions();
         assertEquals(0, preparedTransactions.length);

         if (heuristicCommit)
         {
            assertEquals(1, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

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

         assertEquals(0, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

         server.stop();
         
         server.start();

         jmxServer = ManagementControlHelper.createHornetQServerControl(mbeanServer);
         if (heuristicCommit)
         {
            String[] listHeuristicCommittedTransactions = jmxServer.listHeuristicCommittedTransactions();
            assertEquals(1, listHeuristicCommittedTransactions.length);
            System.out.println(listHeuristicCommittedTransactions[0]);
         } else
         {
            String[] listHeuristicRolledBackTransactions = jmxServer.listHeuristicRolledBackTransactions();
            assertEquals(1, listHeuristicRolledBackTransactions.length);
            System.out.println(listHeuristicRolledBackTransactions[0]);
         }
         
         session = sf.createSession(true, false, false);
         Xid[] recoveredXids = session.recover(XAResource.TMSTARTRSCAN);
         assertEquals(1, recoveredXids.length);
         assertEquals(xid, recoveredXids[0]);         
         assertEquals(0, session.recover(XAResource.TMENDRSCAN).length);
      }
      finally
      {
         if (server.isStarted())
         {            
            server.stop();
         }
      }
   }
   
   public void testForgetHeuristicCommitAndRestart() throws Exception
   {
      doForgetHeuristicCompletedTxAndRestart(true);
   }

   public void testForgetHeuristicRollbackAndRestart() throws Exception
   {
      doForgetHeuristicCompletedTxAndRestart(false);
   }

   private void doForgetHeuristicCompletedTxAndRestart(boolean heuristicCommit) throws Exception
   {
      Configuration configuration = createDefaultConfig();
      configuration.setJMXManagementEnabled(true);

      HornetQServer server = createServer(true, configuration, mbeanServer, new HashMap<String, AddressSettings>());
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

         HornetQServerControl jmxServer = ManagementControlHelper.createHornetQServerControl(mbeanServer);

         String preparedTransactions[] = jmxServer.listPreparedTransactions();

         assertEquals(1, preparedTransactions.length);
         System.out.println(preparedTransactions[0]);

         if (heuristicCommit)
         {
            jmxServer.commitPreparedTransaction(XidImpl.toBase64String(xid));
         }
         else
         {
            jmxServer.rollbackPreparedTransaction(XidImpl.toBase64String(xid));
         }

         preparedTransactions = jmxServer.listPreparedTransactions();
         assertEquals(0, preparedTransactions.length);

         session.forget(xid);

         session.close();

         if (heuristicCommit)
         {
          assertEquals(0, jmxServer.listHeuristicCommittedTransactions().length);  
         }
         else
         {
            assertEquals(0, jmxServer.listHeuristicRolledBackTransactions().length);              
         }
         

         server.stop();
         
         server.start();

         session = sf.createSession(true, false, false);
         Xid[] recoveredXids = session.recover(XAResource.TMSTARTRSCAN);
         assertEquals(0, recoveredXids.length);
         jmxServer = ManagementControlHelper.createHornetQServerControl(mbeanServer);
         if (heuristicCommit)
         {
          assertEquals(0, jmxServer.listHeuristicCommittedTransactions().length);  
         }
         else
         {
            assertEquals(0, jmxServer.listHeuristicRolledBackTransactions().length);              
         }
         
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
