/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.tests.integration.ra;

import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.security.Role;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.ra.*;
import org.hornetq.utils.UUIDGenerator;

import javax.jms.*;
import javax.jms.IllegalStateException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jul 7, 2010
 */
public class OutgoingConnectionTest extends HornetQRATestBase
{
   @Override
   public boolean isSecure()
   {
      return true;
   }

   @Override
   protected void setUp() throws Exception
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
   }

   public void testSimpleMessageSendAndReceive() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(qResourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      Session s = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue q = HornetQJMSClient.createQueue(MDBQUEUE);
      MessageProducer mp = s.createProducer(q);
      MessageConsumer consumer = s.createConsumer(q);
      Message message = s.createTextMessage("test");
      mp.send(message);
      queueConnection.start();
      TextMessage textMessage = (TextMessage) consumer.receive(1000);
      assertNotNull(textMessage);
      assertEquals(textMessage.getText(), "test");
      s.close();
   }

   public void testSimpleMessageSendAndReceiveXA() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(qResourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      XAQueueConnection queueConnection = qraConnectionFactory.createXAQueueConnection();
      XASession s = queueConnection.createXASession();

      XAResource resource = s.getXAResource();
      resource.start(xid, XAResource.TMNOFLAGS);
      Queue q = HornetQJMSClient.createQueue(MDBQUEUE);
      MessageProducer mp = s.createProducer(q);
      MessageConsumer consumer = s.createConsumer(q);
      Message message = s.createTextMessage("test");
      mp.send(message);
      queueConnection.start();
      TextMessage textMessage = (TextMessage) consumer.receiveNoWait();
      assertNull(textMessage);
      resource.end(xid, XAResource.TMSUCCESS);
      resource.commit(xid, true);
      resource.start(xid, XAResource.TMNOFLAGS);
      textMessage = (TextMessage) consumer.receiveNoWait();
      resource.end(xid, XAResource.TMSUCCESS);
      resource.commit(xid, true);
      assertNotNull(textMessage);
      assertEquals(textMessage.getText(), "test");
      s.close();
   }

   public void testSimpleMessageSendAndReceiveTransacted() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      qResourceAdapter.setUseLocalTx(true);
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(qResourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      Session s = queueConnection.createSession(true, Session.AUTO_ACKNOWLEDGE);
      Queue q = HornetQJMSClient.createQueue(MDBQUEUE);
      MessageProducer mp = s.createProducer(q);
      MessageConsumer consumer = s.createConsumer(q);
      Message message = s.createTextMessage("test");
      mp.send(message);
      s.commit();
      queueConnection.start();
      TextMessage textMessage = (TextMessage) consumer.receive(1000);
      assertNotNull(textMessage);
      assertEquals(textMessage.getText(), "test");
      s.rollback();
      textMessage = (TextMessage) consumer.receive(1000);
      assertNotNull(textMessage);
      assertEquals(textMessage.getText(), "test");
      s.commit();
      s.close();
   }

   public void testMultipleSessionsThrowsException() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(qResourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      Session s = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         Session s2 = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         fail("should throw javax,jms.IllegalStateException: Only allowed one session per connection. See the J2EE spec, e.g. J2EE1.4 Section 6.6");
      }
      catch (JMSException e)
      {
         assertTrue(e.getLinkedException() instanceof IllegalStateException);
      }
      s.close();
   }

   public void testConnectionCredentials() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(qResourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE).close();
      queueConnection.close();
      queueConnection = qraConnectionFactory.createQueueConnection("testuser", "testpassword");
      queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE).close();
   }

   public void testConnectionCredentialsFail() throws Exception
   {
      HornetQResourceAdapter qResourceAdapter = new HornetQResourceAdapter();
      qResourceAdapter.setConnectorClassName(InVMConnectorFactory.class.getName());
      MyBootstrapContext ctx = new MyBootstrapContext();
      qResourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(qResourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE).close();
      queueConnection.close();
      queueConnection = qraConnectionFactory.createQueueConnection("testuser", "testwrongpassword");
      try
      {
         queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE).close();
         fail("should throw esxception");
      }
      catch (JMSException e)
      {
         //pass
      }
   }
}
