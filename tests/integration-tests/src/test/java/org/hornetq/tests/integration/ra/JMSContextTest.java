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
package org.hornetq.tests.integration.ra;

import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.security.Role;
import org.hornetq.ra.HornetQRAConnectionFactory;
import org.hornetq.ra.HornetQRAConnectionFactoryImpl;
import org.hornetq.ra.HornetQRAConnectionManager;
import org.hornetq.ra.HornetQRAManagedConnectionFactory;
import org.hornetq.ra.HornetQResourceAdapter;
import org.jboss.security.plugins.TransactionManagerLocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.HashSet;
import java.util.Set;

public class JMSContextTest extends HornetQRATestBase
{
   private HornetQResourceAdapter resourceAdapter;

   HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
   private HornetQRAConnectionFactory qraConnectionFactory;

   private static DummyTransactionManager tm = new DummyTransactionManager();;

   public TransactionManager getTm()
   {
      return tm;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server.getSecurityManager().addUser("testuser", "testpassword");
      server.getSecurityManager().addUser("guest", "guest");
      server.getSecurityManager().setDefaultUser("guest");
      server.getSecurityManager().addRole("testuser", "arole");
      server.getSecurityManager().addRole("guest", "arole");
      Role role = new Role("arole", true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      server.getSecurityRepository().addMatch(MDBQUEUEPREFIXED, roles);
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setTransactionManagerLocatorClass(JMSContextTest.class.getName());
      resourceAdapter.setTransactionManagerLocatorMethod("getTm");

      resourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      tm.tx = null;
      if (resourceAdapter != null)
      {
         resourceAdapter.stop();
      }

      qraConnectionManager.stop();
      super.tearDown();
   }

   @Test
   public void testCreateContextThrowsException() throws Exception
   {
      JMSContext jmsctx = qraConnectionFactory.createContext();
      try
      {
         jmsctx.createContext(JMSContext.AUTO_ACKNOWLEDGE);
         fail("expected JMSRuntimeException");
      }
      catch (JMSRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("wrong exception thrown: " + e);
      }
   }

   @Test
   public void testCreateXAContextThrowsException() throws Exception
   {
      JMSContext jmsctx = qraConnectionFactory.createXAContext();
      try
      {
         jmsctx.createContext(JMSContext.AUTO_ACKNOWLEDGE);
         fail("expected JMSRuntimeException");
      }
      catch (JMSRuntimeException e)
      {
         //pass
      }
      catch (Exception e)
      {
         fail("wrong exception thrown: " + e);
      }
   }

   @Test
   public void sessionTransactedTestActiveJTATx() throws Exception
   {
      try
      {
         qraConnectionFactory.createContext(JMSContext.SESSION_TRANSACTED);
         fail();
      }
      catch (JMSRuntimeException e)
      {
         //pass
      }
   }

   @Test
   public void sessionTransactedTestNoActiveJTATx() throws Exception
   {
      tm.tx = new DummyTransaction();
      JMSContext context = qraConnectionFactory.createContext(JMSContext.SESSION_TRANSACTED);
      assertEquals(context.getSessionMode(), JMSContext.AUTO_ACKNOWLEDGE);
   }

   @Test
   public void clientAckTestActiveJTATx() throws Exception
   {try
      {
         qraConnectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
         fail();
      }
      catch (JMSRuntimeException e)
      {
         //pass
      }
   }

   @Test
   public void clientAckTestNoActiveJTATx() throws Exception
   {
      tm.tx = new DummyTransaction();
      JMSContext context = qraConnectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
      assertEquals(context.getSessionMode(), JMSContext.AUTO_ACKNOWLEDGE);
   }

   private static class DummyTransaction implements Transaction
   {
      @Override
      public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, SystemException
      {
      }

      @Override
      public void rollback() throws IllegalStateException, SystemException
      {
      }

      @Override
      public void setRollbackOnly() throws IllegalStateException, SystemException
      {
      }

      @Override
      public int getStatus() throws SystemException
      {
         return 0;
      }

      @Override
      public boolean enlistResource(XAResource xaResource) throws RollbackException, IllegalStateException, SystemException
      {
         return false;
      }

      @Override
      public boolean delistResource(XAResource xaResource, int i) throws IllegalStateException, SystemException
      {
         return false;
      }

      @Override
      public void registerSynchronization(Synchronization synchronization) throws RollbackException, IllegalStateException, SystemException
      {
      }
   }
   private static class DummyTransactionManager implements TransactionManager
   {
      public Transaction tx;

      @Override
      public void begin() throws NotSupportedException, SystemException
      {
      }

      @Override
      public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException
      {
      }

      @Override
      public void rollback() throws IllegalStateException, SecurityException, SystemException
      {
      }

      @Override
      public void setRollbackOnly() throws IllegalStateException, SystemException
      {
      }

      @Override
      public int getStatus() throws SystemException
      {
         return 0;
      }

      @Override
      public Transaction getTransaction() throws SystemException
      {
         return tx;
      }

      @Override
      public void setTransactionTimeout(int i) throws SystemException
      {
      }

      @Override
      public Transaction suspend() throws SystemException
      {
         return null;
      }

      @Override
      public void resume(Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException
      {
      }
   }
}
