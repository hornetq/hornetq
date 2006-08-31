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
package org.jboss.test.messaging.jms.server.destination;

import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.test.messaging.jms.server.destination.base.DestinationManagementTestBase;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * Tests a queue's management interface.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueueManagementTest extends DestinationManagementTestBase
{
   // Constants -----------------------------------------------------
   private static final String MESSAGE_TWO = "message two";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueManagementTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   public void testReloadQueue() throws Exception
   {      
      String config =
         "<mbean code=\"org.jboss.jms.server.destination.Queue\" " +
         "       name=\"somedomain:service=Queue,name=ReloadQueue\"" +
         "       xmbean-dd=\"xmdesc/Queue-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "</mbean>";
      
      ObjectName destObjectName = deploy(config);

      assertEquals("ReloadQueue", ServerManagement.getAttribute(destObjectName, "Name"));

      String jndiName = "/queue/ReloadQueue";
      String s = (String)ServerManagement.getAttribute(destObjectName, "JNDIName");
      assertEquals(jndiName, s);
      
      //Send some messages
      
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      Queue queue = (Queue)ic.lookup("/queue/ReloadQueue");

      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
  
      for (int i = 0; i < 10; i++)
      {
         TextMessage tm = sess.createTextMessage();
         
         tm.setText("message:" + i);
         
         prod.send(tm);
      }
      
      conn.close();
      
      //Receive half of them
      
      conn = cf.createConnection();
      
      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sess.createConsumer(queue);
      
      conn.start();
      
      for (int i = 0; i < 5; i++)
      {
         TextMessage tm = (TextMessage)cons.receive(1000);
         
         assertNotNull(tm);
         
         assertEquals("message:" + i, tm.getText());
      }
      
      conn.close();
      
      //Undeploy and redeploy the queue
      //The last 5 persistent messages should still be there
      
      undeployDestination("ReloadQueue");
      
      deploy(config);
      
      queue = (Queue)ic.lookup("/queue/ReloadQueue");
      
      conn = cf.createConnection();
      
      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      cons = sess.createConsumer(queue);
      
      conn.start();
      
      for (int i = 5; i < 10; i++)
      {
         TextMessage tm = (TextMessage)cons.receive(1000);
         
         assertNotNull(tm);
         
         assertEquals("message:" + i, tm.getText());
      }
      
      conn.close();      
      
      undeployDestination("ReloadQueue");
   }

   public void testMessageCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      ServerManagement.deployQueue("QueueMessageCount");
      
      try
      {
         Queue queue = (Queue)ic.lookup("/queue/QueueMessageCount");
         
         // Test MessageCount, should be 0 msg
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueMessageCount");
         Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(0, count.intValue());
         
         // Send 1 message to queue
         Connection conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         TextMessage m = session.createTextMessage("message one");
         prod.send(m);
         conn.close();           
         
         // Test MessageCount again, should be 1 msg
         count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(1, count.intValue());
         
         
         // Consume the message
         conn = cf.createConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue);
         conn.start();
         
         cons.receive();
         conn.close();
         
         //Need to pause for a bit since the message is not necessarily removed
         //in memory until sometime after receive has completed
         Thread.sleep(1000);
         
         // Test MessageCount again, should be 0 msg
         count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(0, count.intValue());
      }
      finally
      {
      
         ServerManagement.undeployQueue("QueueMessageCount");
      }
   }

   public void testMessageCountOverFullSize() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Connection conn = null;

      int fullSize = 10;

      ServerManagement.deployQueue("QueueMessageCount2", fullSize, fullSize / 2, fullSize / 2 - 1);

      ObjectName destObjectName =
         new ObjectName("jboss.messaging.destination:service=Queue,name=QueueMessageCount2");

      try
      {
         Queue queue = (Queue)ic.lookup("/queue/QueueMessageCount2");

         conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         // Send 20 message to the queue

         for(int i = 0; i < 20; i++)
         {
            TextMessage m = session.createTextMessage("message" + i);
            prod.send(m);
         }

         int mc =
            ((Integer)ServerManagement.getAttribute(destObjectName, "MessageCount")).intValue();

         assertEquals(20, mc);
      }
      finally
      {
         ServerManagement.undeployQueue("QueueMessageCount2");

         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // TODO this test should be done in DestinationManagementTestBase, once implemented in Topic
   // TODO this only tests reliable non-tx messages
   public void testRemoveAllMessages() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      ServerManagement.deployQueue("QueueRemoveMessages");
      
      try
      {
         Queue queue = (Queue)ic.lookup("/queue/QueueRemoveMessages");
         
         // Send 1 message to queue
         Connection conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         TextMessage m = session.createTextMessage("message one");
         prod.send(m);
         
         // Remove all messages from the queue
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueRemoveMessages");
         ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);
   
         // Test MessageCount again, should be 0 msg
         Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(0, count.intValue());
   
         // Send another message
         m = session.createTextMessage(MESSAGE_TWO);
         prod.send(m);
         conn.close();
   
         // Consume the 2nd message
         conn = cf.createConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue);
         conn.start();
         
         Object ms = cons.receive();
         assertTrue(ms instanceof TextMessage);
         assertEquals(((TextMessage)ms).getText(), MESSAGE_TWO);
         Thread.sleep(500);
         assertNull(cons.receiveNoWait());
         conn.close();
      }
      finally
      {      
         ServerManagement.undeployQueue("QueueRemoveMessages");
      }
   }
   
   public void testListMessages() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      ServerManagement.deployQueue("QueueListMessages");
      
      try
      {
         
         Queue queue = (Queue)ic.lookup("/queue/QueueListMessages");
         
         // Test listMessages, should be 0 msg
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueListMessages");
         
         List list = (List)ServerManagement.invoke(
               destObjectName, 
               "listMessages", 
               new Object[] {null}, 
               new String[] {"java.lang.String"});
         assertNotNull(list);
         assertEquals(0, list.size());
         
         // Send 1 message to queue
         Connection conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         TextMessage m = session.createTextMessage("message one");
         prod.send(m);
         conn.close();
           
         // Test listMessages again, should be 1 msg
         list = (List)ServerManagement.invoke(
               destObjectName, 
               "listMessages", 
               new Object[] {null}, 
               new String[] {"java.lang.String"});
         assertEquals(1, list.size());
         assertTrue(list.get(0) instanceof TextMessage);
         TextMessage m1 = (TextMessage)list.get(0);
         assertEquals(m1.getText(), m.getText());
         
         // Consume the message
         conn = cf.createConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue);
         conn.start();
         
         cons.receive();
         conn.close();
         
         // Test MessageCount again, should be 0 msg
         list = (List)ServerManagement.invoke(
               destObjectName, 
               "listMessages", 
               new Object[] {null}, 
               new String[] {"java.lang.String"});
         assertEquals(0, list.size());
      }
      finally
      {
      
         ServerManagement.undeployQueue("QueueListMessages");
      }
   }

   /**
    * The jmx-console has the habit of sending an empty string if no argument is specified, so
    * we test this eventuality.
    */
   public void testListMessagesEmptySelector() throws Exception
   {
      ServerManagement.deployQueue("QueueListMessages");

      ObjectName destObjectName =
         new ObjectName("jboss.messaging.destination:service=Queue,name=QueueListMessages");

      List list = (List)ServerManagement.invoke(destObjectName,
                                                "listMessages",
                                                new Object[] {""},
                                                new String[] {"java.lang.String"});
      assertNotNull(list);
      assertEquals(0, list.size());

      list = (List)ServerManagement.invoke(destObjectName,
                                                "listMessages",
                                                new Object[] {"             "},
                                                new String[] {"java.lang.String"});
      assertNotNull(list);
      assertEquals(0, list.size());

      ServerManagement.undeployQueue("QueueListMessages");
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return true;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
