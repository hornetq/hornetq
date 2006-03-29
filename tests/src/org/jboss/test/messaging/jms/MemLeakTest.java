/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServer;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.ConnectionListener;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.InvokerLocator;
import org.jboss.remoting.ServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.Connector;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.util.id.GUID;

/**
 * 
 * These tests should be run manually with a profiler running.
 * After running the heap should be inspected to ensure there are no unexpected objects.
 * Ideally we would automate this.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * MemLeakTest.java,v 1.1 2006/03/29 18:51:23 timfox Exp
 */
public class MemLeakTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------


   private static final Logger log = Logger.getLogger(MemLeakTest.class);


   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      try
      {
         MemLeakTest tests = new MemLeakTest("MemLeakTest");
         tests.setUp();
         tests.testRemotingMemLeaks();
         tests.tearDown();
      }
      catch (Throwable e)
      {
         e.printStackTrace();
      }
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public MemLeakTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      log.info("Setting up");

      log.info("starting sc");
      ServerManagement.start("all");
      log.info("started serviceconatiner");      

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }
   
//   
//   public void testNonTxSendReceiveNP() throws Exception
//   {
//      log.info("Pausing");
//      Thread.sleep(10000);
//      
//      InitialContext initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
//      ConnectionFactory cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
//
//      ServerManagement.deployQueue("Queue", 10000, 1000, 1000);
//      
//      Queue queue = (Queue)initialContext.lookup("/queue/Queue");
//      
//      Connection conn = cf.createConnection();
//
//      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//      MessageProducer prod = sess.createProducer(queue);
//
//      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
//
//      final int NUM_MESSAGES = 100;
//      
//      //send some messages
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         TextMessage m = sess.createTextMessage("hello" + i);
//
//         prod.send(m);
//         
//         log.info("Sent " + i);
//
//      }
//      
//      conn.start();
//      
//      MessageConsumer cons = sess.createConsumer(queue);
//      
//      //receive
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         Message m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//            
//      conn.close();
//      
//      conn = null;
//      
//      sess = null;
//      
//      prod = null;
//      
//      cons = null;
//      
//      queue = null;
//      
//      cf = null;
//      
//      Thread.sleep(20 * 60 * 1000);
//   }
//   
//   public void testManyConns() throws Exception
//   {
//      log.info("Pausing");
//      Thread.sleep(10000);
//      
//      InitialContext initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
//      ConnectionFactory cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
//
//      ServerManagement.deployQueue("Queue", 10000, 1000, 1000);
//      
//      Queue queue = (Queue)initialContext.lookup("/queue/Queue");
//      
//      
//      for (int i = 0; i < 200; i++)
//      {
//      
//         
//         Connection conn = cf.createConnection();
//   
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//   
//         MessageProducer prod = sess.createProducer(queue);
//   
//         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
//   
//         //send a message
//         
//         Message m = sess.createTextMessage("hello" + i);
//   
//         prod.send(m);
//         
//         log.info("Sent " + i);
//   
//         conn.start();
//         
//         MessageConsumer cons = sess.createConsumer(queue);
//         
//         //receive
//         
//         m = cons.receive();
//                              
//         conn.close();
//         
//         log.info("i:" + i);
//      }
//
//      queue = null;
//      
//      cf = null;
//      
//      Thread.sleep(20 * 60 * 1000);
//   }
//   
//   public void testNonTxSendReceiveP() throws Exception
//   {
//      log.info("Pausing");
//      Thread.sleep(10000);
//      
//      InitialContext initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
//      ConnectionFactory cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
//
//      ServerManagement.deployQueue("Queue", 10000, 1000, 1000);
//      
//      Queue queue = (Queue)initialContext.lookup("/queue/Queue");
//      
//      Connection conn = cf.createConnection();
//
//      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//      MessageProducer prod = sess.createProducer(queue);
//
//      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
//
//      final int NUM_MESSAGES = 1000;
//      
//      //send some messages
//      
//      String s = new GUID().toString();
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         TextMessage m = sess.createTextMessage(s);
//
//         prod.send(m);
//         
//         log.info("Sent " + i);
//
//      }
//      
//      conn.start();
//      
//      MessageConsumer cons = sess.createConsumer(queue);
//      
//      //receive
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         Message m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//            
//      conn.close();
//      
//      conn = null;
//      
//      sess = null;
//      
//      prod = null;
//      
//      cons = null;
//      
//      queue = null;
//      
//      cf = null;
//      
//      Thread.sleep(20 * 60 * 1000);
//   }
//   
//   
//   public void testTxSendReceiveNP() throws Exception
//   {
//      log.info("Pausing");
//      Thread.sleep(10000);
//      
//      InitialContext initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
//      ConnectionFactory cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
//
//      ServerManagement.deployQueue("Queue", 10000, 1000, 1000);
//      
//      Queue queue = (Queue)initialContext.lookup("/queue/Queue");
//      
//      Connection conn = cf.createConnection();
//
//      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
//
//      MessageProducer prod = sess.createProducer(queue);
//
//      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
//
//      final int NUM_MESSAGES = 100;
//      
//      //send some messages
//      
//      String s = new GUID().toString();
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         TextMessage m = sess.createTextMessage(s);
//
//         prod.send(m);
//         
//         log.info("Sent " + i);
//
//      }
//      
//      sess.rollback();
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         TextMessage m = sess.createTextMessage(s);
//
//         prod.send(m);
//         
//         log.info("Sent " + i);
//
//      }
//      
//      sess.commit();
//      
//      conn.start();
//      
//      MessageConsumer cons = sess.createConsumer(queue);
//      
//      //receive
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         Message m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//      
//      sess.rollback();
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         Message m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//      
//      sess.commit();
//                        
//      conn.close();
//      
//      conn = null;
//      
//      sess = null;
//      
//      prod = null;
//      
//      cons = null;
//      
//      queue = null;
//      
//      cf = null;
//      
//      Thread.sleep(20 * 60 * 1000);
//   }
//   
//   public void testTxSendReceiveP() throws Exception
//   {
//      log.info("Pausing");
//      Thread.sleep(10000);
//      
//      InitialContext initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
//      ConnectionFactory cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
//
//      ServerManagement.deployQueue("Queue", 10000, 1000, 1000);
//      
//      Queue queue = (Queue)initialContext.lookup("/queue/Queue");
//      
//      Connection conn = cf.createConnection();
//
//      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
//
//      MessageProducer prod = sess.createProducer(queue);
//
//      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
//
//      final int NUM_MESSAGES = 100;
//      
//      //send some messages
//      
//      String s = new GUID().toString();
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         TextMessage m = sess.createTextMessage(s);
//
//         prod.send(m);
//         
//         log.info("Sent " + i);
//
//      }
//      
//      sess.rollback();
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         TextMessage m = sess.createTextMessage(s);
//
//         prod.send(m);
//         
//         log.info("Sent " + i);
//
//      }
//      
//      sess.commit();
//      
//      conn.start();
//      
//      MessageConsumer cons = sess.createConsumer(queue);
//      
//      //receive
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         Message m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//      
//      sess.rollback();
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         Message m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//      
//      sess.commit();
//                        
//      conn.close();
//      
//      conn = null;
//      
//      sess = null;
//      
//      prod = null;
//      
//      cons = null;
//      
//      queue = null;
//      
//      cf = null;
//      
//      Thread.sleep(20 * 60 * 1000);
//   }
//   
//   
//   public void testSendReceiveClientAckNP() throws Exception
//   {
//      log.info("Pausing");
//      Thread.sleep(10000);
//      
//      InitialContext initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
//      ConnectionFactory cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
//
//      ServerManagement.deployQueue("Queue", 10000, 1000, 1000);
//      
//      Queue queue = (Queue)initialContext.lookup("/queue/Queue");
//      
//      Connection conn = cf.createConnection();
//
//      Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//
//      MessageProducer prod = sess.createProducer(queue);
//
//      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
//
//      final int NUM_MESSAGES = 100;
//      
//      //send some messages
//      
//      for (int i = 0; i < NUM_MESSAGES; i++)
//      {
//
//         TextMessage m = sess.createTextMessage("hello" + i);
//
//         prod.send(m);
//         
//         log.info("Sent " + i);
//
//      }
//      
//      conn.start();
//      
//      MessageConsumer cons = sess.createConsumer(queue);
//      
//      //receive
//      
//      Message m = null;
//      
//      for (int i = 0; i < NUM_MESSAGES / 2; i++)
//      {
//
//         m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//      
//      m.acknowledge();
//      
//      for (int i = 0; i < NUM_MESSAGES / 2; i++)
//      {
//
//         m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//      
//      //don't ack
//      
//      sess.close();
//      
//      sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//      
//      cons = sess.createConsumer(queue);
//      
//      //ack the rest
//      
//      for (int i = 0; i < NUM_MESSAGES / 2; i++)
//      {
//
//         m = cons.receive();
//         
//         log.info("Received " + i);
//
//      }
//      
//      m.acknowledge();
//                 
//      conn.close();
//      
//      conn = null;
//      
//      sess = null;
//      
//      prod = null;
//      
//      cons = null;
//      
//      queue = null;
//      
//      cf = null;
//      
//      Thread.sleep(20 * 60 * 1000);
//   }
   
   class SimpleConnectionListener implements ConnectionListener
   {
      public void handleConnectionException(Throwable arg0, Client arg1)
      {         
      }      
   }
   
   public void testRemotingMemLeaks() throws Throwable
   {
      log.info("Test remoting mem leaks");
      
      Thread.sleep(10 * 1000);
      
      Connector serverConnector = new Connector();

      InvokerLocator serverLocator = new InvokerLocator("socket://localhost:9099");

      serverConnector.setInvokerLocator(serverLocator.getLocatorURI());

      serverConnector.create();
      
      serverConnector.setLeasePeriod(5000);
      
      serverConnector.addConnectionListener(new SimpleConnectionListener());

      SimpleServerInvocationHandler invocationHandler = new SimpleServerInvocationHandler();

      serverConnector.addInvocationHandler("JMS", invocationHandler);

      serverConnector.start();

      InvokerLocator serverLocator2 = new InvokerLocator("socket://localhost:9099/forceRemoting=true&leasing=true");
      
      for (int i = 0; i < 500; i++)
      {
         Client cl = new Client(serverLocator2);
         
         cl.connect();
         
         for (int j = 0; j < 100; j++)
         {
            cl.invoke("pickled onions");
         }
         
         cl.disconnect();
      }
     
      serverConnector.stop();
      
      serverConnector.destroy();      
      
      log.info("done");
      
      Thread.sleep(20 * 60 * 1000);
   }

   // Public --------------------------------------------------------
   
   class SimpleServerInvocationHandler implements ServerInvocationHandler
   {
      InvokerCallbackHandler handler;

      public void addListener(InvokerCallbackHandler callbackHandler)
      {
         this.handler = callbackHandler;

      }

      public Object invoke(InvocationRequest invocation) throws Throwable
      {
         //log.info("Received invocation:" + invocation);

         return "Sausages";
      }

      public void removeListener(InvokerCallbackHandler callbackHandler)
      {
         // FIXME removeListener

      }

      public void setInvoker(ServerInvoker invoker)
      {
         // FIXME setInvoker

      }

      public void setMBeanServer(MBeanServer server)
      {
         // FIXME setMBeanServer

      }

   }
}
