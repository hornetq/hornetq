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

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.XAQueueConnection;
import javax.jms.XASession;
import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.security.Role;
import org.hornetq.core.transaction.impl.XidImpl;
import org.hornetq.ra.HornetQRAConnectionFactory;
import org.hornetq.ra.HornetQRAConnectionFactoryImpl;
import org.hornetq.ra.HornetQRAConnectionManager;
import org.hornetq.ra.HornetQRAManagedConnectionFactory;
import org.hornetq.ra.HornetQRASession;
import org.hornetq.ra.HornetQResourceAdapter;
import org.hornetq.utils.UUIDGenerator;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jul 7, 2010
 */
public class OutgoingConnectionTest extends HornetQRATestBase
{
   private HornetQResourceAdapter resourceAdapter;

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

   @Override
   protected void tearDown() throws Exception
   {
      if (resourceAdapter != null)
      {
         resourceAdapter.stop();
      }
      super.tearDown();
   }

   public void testIllegalStateDurableSubscriber() throws Exception
   {
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      QueueSession s = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      Topic topic = HornetQJMSClient.createTopic(MDBQUEUE);
      try
      {
         s.createDurableSubscriber(topic, "test", "test", true);
         fail("createDurableSubscriber on QueueSession should throw IllegalStateException");
      }
      catch (IllegalStateException e)
      {
         //success
      }

      ManagedConnection mc = ((HornetQRASession)s).getManagedConnection();
      s.close();
      mc.destroy();
   }

   public void testIllegalStateQueueBrowser() throws Exception
   {
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      TopicConnection topicConnection = qraConnectionFactory.createTopicConnection();
      TopicSession t = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue q = HornetQJMSClient.createQueue(MDBQUEUE);
      try
      {
         t.createBrowser(q);
         fail("createDurableSubscriber on QueueSession should throw IllegalStateException");
      }
      catch (IllegalStateException e)
      {
         //success
      }

      ManagedConnection mc = ((HornetQRASession)t).getManagedConnection();
      t.close();
      mc.destroy();
   }

   public void testSendProperties() throws Exception
   {
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      QueueSession qs = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue q = HornetQJMSClient.createQueue(MDBQUEUE);
      queueConnection.start();
      MessageProducer mp = qs.createProducer(q);
      QueueReceiver qr = qs.createReceiver(q);

        boolean     bool = true;
      byte        bValue = 127;
        short       nShort = 10;
        int         nInt = 5;
        long        nLong = 333;
        float       nFloat = 1;
        double      nDouble = 100;
        String      testString = "test";
        Enumeration propertyNames = null;
        Enumeration jmsxDefined = null;
        int         numPropertyNames = 17;
        String      testMessageBody = "Testing...";

        try {
            TextMessage messageSent = null;
            TextMessage messageReceived = null;

            // set up test tool for Queue
            messageSent = qs.createTextMessage();
            messageSent.setText(testMessageBody);

            // ------------------------------------------------------------------------------
            // set properties for boolean, byte, short, int, long, float, double, and String.
            // ------------------------------------------------------------------------------
            messageSent.setBooleanProperty("TESTBOOLEAN", bool);
            messageSent.setByteProperty("TESTBYTE", bValue);
            messageSent.setShortProperty("TESTSHORT", nShort);
            messageSent.setIntProperty("TESTINT", nInt);
            messageSent.setFloatProperty("TESTFLOAT", nFloat);
            messageSent.setDoubleProperty("TESTDOUBLE", nDouble);
            messageSent.setStringProperty("TESTSTRING", "test");
            messageSent.setLongProperty("TESTLONG", nLong);

            // ------------------------------------------------------------------------------
            // set properties for Boolean, Byte, Short, Int, Long, Float, Double, and String.
            // ------------------------------------------------------------------------------
            messageSent.setObjectProperty("OBJTESTBOOLEAN", Boolean.valueOf(bool) );
            messageSent.setObjectProperty("OBJTESTBYTE", new Byte(bValue));
            messageSent.setObjectProperty("OBJTESTSHORT", new Short(nShort));
            messageSent.setObjectProperty("OBJTESTINT", new Integer(nInt));
            messageSent.setObjectProperty("OBJTESTFLOAT", new Float(nFloat));
            messageSent.setObjectProperty("OBJTESTDOUBLE", new Double(nDouble));
            messageSent.setObjectProperty("OBJTESTSTRING", "test");
            messageSent.setObjectProperty("OBJTESTLONG", new Long(nLong));
            messageSent.setStringProperty("COM_SUN_JMS_TESTNAME", "msgPropertiesQTest");
           mp.send(messageSent);
           messageReceived = (TextMessage) qr.receive(5000);
           assertNotNull(messageReceived);
           // iterate thru the property names
            int i = 0;
            propertyNames = messageReceived.getPropertyNames();
            do {
                String tmp = (String)propertyNames.nextElement();
                System.out.println("+++++++   Property Name is: " + tmp );
                if ( tmp.indexOf("JMS") != 0 )
                    i++;
            } while (propertyNames.hasMoreElements());

            if (i == numPropertyNames) {
                System.out.println("Pass: # of properties is " + numPropertyNames + " as expected");
            } else {
                System.out.println("Error: expected " + numPropertyNames + "property names");
                System.out.println("       But " + i + " returned");
            }
        }
        catch (Exception e)
        {
           e.printStackTrace();
           fail();
        }

      ManagedConnection mc = ((HornetQRASession)qs).getManagedConnection();
      qs.close();
      mc.destroy();
   }

   public void testSimpleMessageSendAndReceive() throws Exception
   {
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
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

      ManagedConnection mc = ((HornetQRASession)s).getManagedConnection();
      s.close();
      mc.destroy();
   }

   public void testSimpleMessageSendAndReceiveXA() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
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

      ManagedConnection mc = ((HornetQRASession)s).getManagedConnection();
      s.close();
      mc.destroy();
   }

   public void testSimpleMessageSendAndReceiveTransacted() throws Exception
   {
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      resourceAdapter.setUseLocalTx(true);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
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

      ManagedConnection mc = ((HornetQRASession)s).getManagedConnection();
      s.close();
      mc.destroy();
   }

   public void testMultipleSessionsThrowsException() throws Exception
   {
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      Session s = queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         queueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         fail("should throw javax,jms.IllegalStateException: Only allowed one session per connection. See the J2EE spec, e.g. J2EE1.4 Section 6.6");
      }
      catch (JMSException e)
      {
         assertTrue(e.getLinkedException() instanceof IllegalStateException);
      }

      ManagedConnection mc = ((HornetQRASession)s).getManagedConnection();
      s.close();
      mc.destroy();
   }

   public void testConnectionCredentials() throws Exception
   {
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      QueueSession session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      ManagedConnection mc = ((HornetQRASession)session).getManagedConnection();
      queueConnection.close();
      mc.destroy();

      queueConnection = qraConnectionFactory.createQueueConnection("testuser", "testpassword");
      session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      mc = ((HornetQRASession)session).getManagedConnection();
      queueConnection.close();
      mc.destroy();

   }

   public void testConnectionCredentialsFail() throws Exception
   {
      resourceAdapter = new HornetQResourceAdapter();
      resourceAdapter.setConnectorClassName(INVM_CONNECTOR_FACTORY);
      MyBootstrapContext ctx = new MyBootstrapContext();
      resourceAdapter.start(ctx);
      HornetQRAConnectionManager qraConnectionManager = new HornetQRAConnectionManager();
      HornetQRAManagedConnectionFactory mcf = new HornetQRAManagedConnectionFactory();
      mcf.setResourceAdapter(resourceAdapter);
      HornetQRAConnectionFactory qraConnectionFactory = new HornetQRAConnectionFactoryImpl(mcf, qraConnectionManager);
      QueueConnection queueConnection = qraConnectionFactory.createQueueConnection();
      QueueSession session = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      ManagedConnection mc = ((HornetQRASession)session).getManagedConnection();
      queueConnection.close();
      mc.destroy();

      queueConnection = qraConnectionFactory.createQueueConnection("testuser", "testwrongpassword");
      try
      {
         queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE).close();
         fail("should throw exception");
      }
      catch (JMSException e)
      {
         //pass
      }
   }
}
