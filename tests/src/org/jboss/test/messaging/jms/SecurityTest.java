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
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.management.ObjectName;
import javax.transaction.xa.XAResource;

import org.jboss.jms.exception.MessagingXAException;
import org.jboss.jms.tx.MessagingXid;
import org.jboss.logging.Logger;
import org.jboss.messaging.util.XMLUtil;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * Test JMS Security.
 * 
 * This test must be run with the Test security config. on the server
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * 
 * Much of the basic idea of the tests come from SecurityUnitTestCase.java in JBossMQ by:
 * @author <a href="pra@tim.se">Peter Antman</a>
 * 
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class SecurityTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SecurityTest.class);
   
   private static final String defConfig = "<security><role name=\"def\" read=\"true\" write=\"true\" create=\"true\"/></security>";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String oldDefaultConfig;
   
   // Constructors --------------------------------------------------

   public SecurityTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   /**
    * Login with no user, no password
    * Should allow login (equivalent to guest)
    */
   public void testLoginNoUserNoPassword() throws Exception
   {

      Connection conn1 = null;
      Connection conn2 = null;
      try
      {
         conn1 = cf.createConnection();
         conn2 = cf.createConnection(null, null);
      }
      finally
      {
         if (conn1 != null) conn1.close();
         if (conn2 != null) conn2.close();
      }
   }

   /**
    * Login with valid user and password
    * Should allow
    */
   public void testLoginValidUserAndPassword() throws Exception
   {
      Connection conn1 = null;
      try
      {
         conn1 = cf.createConnection("john", "needle");
      }
      finally
      {
         if (conn1 != null) conn1.close();
      }
   }

   /**
    * Login with valid user and invalid password
    * Should allow
    */
   public void testLoginValidUserInvalidPassword() throws Exception
   {
      Connection conn1 = null;
      try
      {
         conn1 = cf.createConnection("john", "blobby");
         fail();
      }
      catch (JMSSecurityException e)
      {
         //Expected
      }
      finally
      {
         if (conn1 != null) conn1.close();
      }
   }

   /**
    * Login with invalid user and invalid password
    * Should allow
    */
   public void testLoginInvalidUserInvalidPassword() throws Exception
   {
      Connection conn1 = null;
      try
      {
         conn1 = cf.createConnection("osama", "blah");
         fail();
      }
      catch (JMSSecurityException e)
      {
         //Expected
      }
      finally
      {
         if (conn1 != null) conn1.close();
      }
   }

   /* Now some client id tests */

   /**
    * user/pwd with preconfigured clientID, should return preconf
    */
    public void testPreConfClientID() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("dilbert", "dogbert");
         String clientID = conn.getClientID();
         assertEquals("Invalid ClientID", "dilbert-id", clientID);
      }
      finally
      {
         if (conn != null)
            conn.close();
      }
   }

   /**
    * Try setting client ID
    */
   public void testSetClientID() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection();
         conn.setClientID("myID");
         String clientID = conn.getClientID();
         assertEquals("Invalid ClientID", "myID", clientID);
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   /**
    * Try setting client ID on preconfigured connection - should throw exception
    */
   public void testSetClientIDPreConf() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("dilbert", "dogbert");
         conn.setClientID("myID");
         fail();
      }
      catch (IllegalStateException e)
      {
         // Expected
      }
      finally
      {
         if (conn != null)
            conn.close();
      }
   }

   /*
    * Try setting client ID after an operation has been performed on the connection
    */
   public void testSetClientIDAfterOp() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection();
         conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn.setClientID("myID");
         fail();
      }
      catch (IllegalStateException e)
      {
         //Expected
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   //
   // Authorization tests
   //

   public void testAnonymousConnection() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection();
         assertTrue(canWriteDestination(conn, queue1));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testValidTopicPublisher() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(canWriteDestination(conn, topic1));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testInvalidTopicPublisher() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");
         assertFalse(canWriteDestination(conn, topic1));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testValidTopicSubscriber() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(canReadDestination(conn, topic1));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testInvalidTopicSubscriber() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");
         assertFalse(canReadDestination(conn, topic1));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testValidQueueBrowser() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         sess.createBrowser(queue1);
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testInvalidQueueBrowser() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         sess.createBrowser(queue1);
         fail("should throw JMSSecurityException");
      }
      catch (JMSSecurityException e)
      {
         //Expected
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testValidQueueSender() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(this.canWriteDestination(conn, queue1));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testInvalidQueueSender() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");
         assertFalse(this.canWriteDestination(conn, queue1));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testValidQueueReceiver() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(this.canReadDestination(conn, queue1));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testInvalidQueueReceiver() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");
         assertFalse(this.canReadDestination(conn, queue1));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   /**
    * Test valid durable subscription creation for connection preconfigured with client id
    */
   public void testValidDurableSubscriptionCreationPreConf() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("dilbert", "dogbert");
         assertTrue(this.canCreateDurableSub(conn, topic1, "sub2"));
      }
      finally
      {
         if (conn != null)
            conn.close();
      }
   }

   /*
    * Test invalid durable subscription creation for connection preconfigured with client id
    */

   public void testInvalidDurableSubscriptionCreationPreConf() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("dilbert", "dogbert");
         assertFalse(this.canCreateDurableSub(conn, topic2, "sub3"));
      }
      finally
      {
         if (conn != null)
            conn.close();
      }
   }

   /*
    * Test valid durable subscription creation for connection not preconfigured with client id
    */
   public void testValidDurableSubscriptionCreationNotPreConf() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("dynsub", "dynsub");
         conn.setClientID("myID");
         assertTrue(this.canCreateDurableSub(conn, topic1, "sub4"));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   /*
    * Test invalid durable subscription creation for connection not preconfigured with client id
    */
   public void testInvalidDurableSubscriptionCreationNotPreConf() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("dynsub", "dynsub");
         conn.setClientID("myID2");
         assertFalse(this.canCreateDurableSub(conn, topic2, "sub5"));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testDefaultSecurityValid() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");
         conn.setClientID("myID5");
         assertTrue(this.canReadDestination(conn, topic3));
         assertTrue(this.canWriteDestination(conn, topic3));
         assertTrue(this.canCreateDurableSub(conn, topic3, "subxyz"));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testDefaultSecurityInvalid() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");
         conn.setClientID("myID6");
         assertFalse(this.canReadDestination(conn, topic3));
         assertFalse(this.canWriteDestination(conn, topic3));
         assertFalse(this.canCreateDurableSub(conn, topic3, "subabc"));
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   /**
    * This test makes sure that changing the default security configuration on the server has effect
    * over already deployed destinations.
    */
   public void testDefaultSecurityUpdate() throws Exception
   {
      String defSecConf = ServerManagement.getDefaultSecurityConfig();

      // Make sure it is the default security configuration I rely on
     
      XMLUtil.assertEquivalent(XMLUtil.stringToElement(defConfig),
                               XMLUtil.stringToElement(defSecConf));

      // "john" has the role def, so he should be able to create a producer and a consumer on a queue
      Connection conn = null;

      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(canReadDestination(conn, queue2));
         assertTrue(canWriteDestination(conn, queue2));

         String newSecurityConfig =
            "<security><role name=\"someotherrole\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.setDefaultSecurityConfig(newSecurityConfig);

         assertFalse(canReadDestination(conn, queue2));
         // we should only look non transacted, as looking on connection would require the test
         // to wait 15s (eviction timeout)
         assertFalse(canWriteDestination(conn, queue2, false));

         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"false\" create=\"false\"/></security>";

         ServerManagement.setDefaultSecurityConfig(newSecurityConfig);

         assertTrue(canReadDestination(conn, queue2));
         // to avoid cache evict timeout
         assertFalse(canWriteDestination(conn, queue2, false));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   /**
    * This test makes sure that changing the queue security configuration on the server has effect
    * over destinations when they are stopped (this is what happens in a real deployment - the security config
    * gets set before the queue/topic is started
    * See http://jira.jboss.com/jira/browse/JBMESSAGING-976
    */
   public void testQueueSecurityUpdateStopped() throws Exception
   {
      // "john" has the role def, so he should be able to create a producer and a consumer on a queue

      ObjectName on = new ObjectName("jboss.messaging.destination:service=Queue,name=Queue2");
      
      Connection conn = null;

      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(canReadDestination(conn, queue2));
         assertTrue(canWriteDestination(conn, queue2));

         String newSecurityConfig =
            "<security><role name=\"someotherrole\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.invoke(on, "stop", null, null);         
         ServerManagement.configureSecurityForDestination("Queue2", newSecurityConfig);         
         ServerManagement.invoke(on, "start", null, null);
         
         assertFalse(canReadDestination(conn, queue2));
         // non transacted to avoid evict timeout
         assertFalse(canWriteDestination(conn, queue2, false));


         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"false\" create=\"false\"/></security>";

         ServerManagement.invoke(on, "stop", null, null);         
         ServerManagement.configureSecurityForDestination("Queue2", newSecurityConfig);         
         ServerManagement.invoke(on, "start", null, null);

         assertTrue(canReadDestination(conn, queue2));
         assertFalse(canWriteDestination(conn, queue2, false));

         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.invoke(on, "stop", null, null);         
         ServerManagement.configureSecurityForDestination("Queue2", newSecurityConfig);         
         ServerManagement.invoke(on, "start", null, null);

         assertTrue(canReadDestination(conn, queue2));
         assertTrue(canWriteDestination(conn, queue2, false));
      }
      finally
      {      	         
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   /**
    * This test makes sure that changing the topic security configuration on the server has effect
    * over destinations when they are stopped (this is what happens in a real deployment - the security config
    * gets set before the queue/topic is started
    * See http://jira.jboss.com/jira/browse/JBMESSAGING-976
    */
   public void testTopicSecurityUpdateStopped() throws Exception
   {
      // "john" has the role def, so he should be able to create a producer and a consumer on a queue

      ObjectName on = new ObjectName("jboss.messaging.destination:service=Topic,name=Topic2");
      
      Connection conn = null;

      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(canReadDestination(conn, topic2));
         assertTrue(canWriteDestination(conn, topic2));


         String newSecurityConfig =
            "<security><role name=\"someotherrole\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.invoke(on, "stop", null, null);         
         ServerManagement.configureSecurityForDestination("Topic2", newSecurityConfig);         
         ServerManagement.invoke(on, "start", null, null);
         
         assertFalse(canReadDestination(conn, topic2));
         assertFalse(canWriteDestination(conn, topic2, false));


         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"false\" create=\"false\"/></security>";

         ServerManagement.invoke(on, "stop", null, null);         
         ServerManagement.configureSecurityForDestination("Topic2", newSecurityConfig);         
         ServerManagement.invoke(on, "start", null, null);

         assertTrue(canReadDestination(conn, topic2));
         assertFalse(canWriteDestination(conn, topic2, false));

         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.invoke(on, "stop", null, null);         
         ServerManagement.configureSecurityForDestination("Topic2", newSecurityConfig);         
         ServerManagement.invoke(on, "start", null, null);

         assertTrue(canReadDestination(conn, topic2));
         assertTrue(canWriteDestination(conn, topic2, false));
      }
      finally
      {      	
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   /**
    * This test makes sure that changing the queue security configuration on the server has effect
    * over already deployed destinations.
    */
   public void testQueueSecurityUpdate() throws Exception
   {
      // "john" has the role def, so he should be able to create a producer and a consumer on a queue
      Connection conn = null;

      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(canReadDestination(conn, queue2));
         assertTrue(canWriteDestination(conn, queue2));

         String newSecurityConfig =
            "<security><role name=\"someotherrole\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.configureSecurityForDestination("Queue2", newSecurityConfig);
         
         assertFalse(canReadDestination(conn, queue2));
         assertFalse(canWriteDestination(conn, queue2, false));


         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"false\" create=\"false\"/></security>";

         ServerManagement.configureSecurityForDestination("Queue2", newSecurityConfig);

         assertTrue(canReadDestination(conn, queue2));
         assertFalse(canWriteDestination(conn, queue2, false));

         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.configureSecurityForDestination("Queue2", newSecurityConfig);

         assertTrue(canReadDestination(conn, queue2));
         assertTrue(canWriteDestination(conn, queue2, false));
         
         //Now set to null
         
         ServerManagement.configureSecurityForDestination("Queue2", null);
         
         //Should fall back to the default config
         String lockedConf = "<security><role name=\"alien\" read=\"true\" write=\"true\" create=\"true\"/></security>";
         
         ServerManagement.setDefaultSecurityConfig(lockedConf);
         
         assertFalse(canReadDestination(conn, queue2));
         assertFalse(canWriteDestination(conn, queue2, false));
         
         ServerManagement.setDefaultSecurityConfig(defConfig);
         
         assertTrue(canReadDestination(conn, queue2));
         assertTrue(canWriteDestination(conn, queue2, false));
      }
      finally
      {         
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   /**
    * This test makes sure that changing the topic security configuration on the server has effect
    * over already deployed destinations.
    */
   public void testTopicSecurityUpdate() throws Exception
   {
      // "john" has the role def, so he should be able to create a producer and a consumer on a queue
      Connection conn = null;

      try
      {
         conn = cf.createConnection("john", "needle");
         assertTrue(canReadDestination(conn, topic2));
         assertTrue(canWriteDestination(conn, topic2));


         String newSecurityConfig =
            "<security><role name=\"someotherrole\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.configureSecurityForDestination("Topic2", newSecurityConfig);
         
         assertFalse(canReadDestination(conn, topic2));
         assertFalse(canWriteDestination(conn, topic2, false));


         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"false\" create=\"false\"/></security>";

         ServerManagement.configureSecurityForDestination("Topic2", newSecurityConfig);

         assertTrue(canReadDestination(conn, topic2));
         assertFalse(canWriteDestination(conn, topic2, false));

         newSecurityConfig =
            "<security><role name=\"def\" read=\"true\" write=\"true\" create=\"false\"/></security>";

         ServerManagement.configureSecurityForDestination("Topic2", newSecurityConfig);

         assertTrue(canReadDestination(conn, topic2));
         assertTrue(canWriteDestination(conn, topic2, false));
         
         //Now set to null
         
         ServerManagement.configureSecurityForDestination("Topic2", null);
         
         //Should fall back to the default config
         String lockedConf = "<security><role name=\"alien\" read=\"true\" write=\"true\" create=\"true\"/></security>";
         
         ServerManagement.setDefaultSecurityConfig(lockedConf);
         
         assertFalse(canReadDestination(conn, topic2));
         assertFalse(canWriteDestination(conn, topic2, false));
         
         ServerManagement.setDefaultSecurityConfig(defConfig);
         
         assertTrue(canReadDestination(conn, topic2));
         assertTrue(canWriteDestination(conn, topic2, false));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testSecurityForQueuesAndTopicsWithTheSameName() throws Exception
   {
      ServerManagement.deployQueue("Accounting");
      ServerManagement.deployTopic("Accounting");

      Connection conn = null;

      try
      {
         // configure the queue to allow "def" to read
         String config = "<security><role name=\"def\" read=\"true\" write=\"false\" create=\"false\"/></security>";
         ObjectName on = new ObjectName("jboss.messaging.destination:service=Queue,name=Accounting");
         ServerManagement.setAttribute(on, "SecurityConfig", config);

         // configure the topic to prevent "def" from reading
         config = "<security><role name=\"def\" read=\"false\" write=\"false\" create=\"false\"/></security>";
         on = new ObjectName("jboss.messaging.destination:service=Topic,name=Accounting");
         ServerManagement.setAttribute(on, "SecurityConfig", config);

         Queue queue = (Queue)ic.lookup("/queue/Accounting");
         Topic topic = (Topic)ic.lookup("/topic/Accounting");

         conn = cf.createConnection("john", "needle");

         assertTrue(canReadDestination(conn, queue));
         assertFalse(canReadDestination(conn, topic));
      }
      finally
      {
         ServerManagement.undeployQueue("Accounting");
         ServerManagement.undeployTopic("Accounting");
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testSecurityForTemporaryQueue() throws Exception
   {
      testSecurityForTemporaryDestination(true);
   }

   public void testSecurityForTemporaryTopic() throws Exception
   {
      testSecurityForTemporaryDestination(false);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      
      oldDefaultConfig = ServerManagement.getDefaultSecurityConfig();
      
      final String queueConf =
         "<security>" +
            "<role name=\"guest\" read=\"true\" write=\"true\"/>" +
            "<role name=\"publisher\" read=\"true\" write=\"true\" create=\"false\"/>" +
            "<role name=\"noacc\" read=\"false\" write=\"false\" create=\"false\"/>" +
         "</security>";

      ServerManagement.configureSecurityForDestination("Queue1", queueConf);

      final String topicConf =
         "<security>" +
            "<role name=\"guest\" read=\"true\" write=\"true\"/>" +
            "<role name=\"publisher\" read=\"true\" write=\"true\" create=\"false\"/>" +
            "<role name=\"durpublisher\" read=\"true\" write=\"true\" create=\"true\"/>" +
         "</security>";

      ServerManagement.configureSecurityForDestination("Topic1", topicConf);


      final String testSecuredTopicConf =
         "<security>" +
            "<role name=\"publisher\" read=\"true\" write=\"true\" create=\"false\"/>" +
         "</security>";

      ServerManagement.configureSecurityForDestination("Topic2", testSecuredTopicConf);

      ServerManagement.setDefaultSecurityConfig(defConfig);
   }

   protected void tearDown() throws Exception
   {
   	super.tearDown();
   	
      ServerManagement.setDefaultSecurityConfig(oldDefaultConfig);
      ServerManagement.configureSecurityForDestination("Queue1", null);
      ServerManagement.configureSecurityForDestination("Queue2", null);
      ServerManagement.configureSecurityForDestination("Topic1", null);
      ServerManagement.configureSecurityForDestination("Topic2", null);
   }

   // Private -------------------------------------------------------

   private boolean canReadDestination(Connection conn, Destination dest) throws Exception
   {
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      try
      {
         sess.createConsumer(dest);
         return true;
      }
      catch (JMSSecurityException e)
      {
         log.trace("Can't read destination", e);
         return false;
      }
      finally
      {
         sess.close();
      }
            
   }
   private boolean canWriteDestination(Connection conn, Destination dest) throws Exception
   {
      boolean transacted = canWriteDestination(conn, dest, true);
      boolean nonTransacted = canWriteDestination(conn, dest, false);

      return transacted || nonTransacted;
   }

   private boolean canWriteDestination(Connection conn, Destination dest, boolean transacted) throws Exception
   {
      Session sess = conn.createSession(transacted, Session.AUTO_ACKNOWLEDGE);

      try
      {
         boolean namedSucceeded = true;
         try
         {
            MessageProducer prod = sess.createProducer(dest);
            Message m = sess.createTextMessage("Kippers");
            prod.send(m);
            if (transacted)
            {
               sess.commit();
            }
         }
         catch (JMSSecurityException e)
         {
            log.trace("Can't write to destination using named producer", e);
            namedSucceeded = false;
         }

         boolean anonSucceeded = true;
         try
         {
            MessageProducer producerAnon = sess.createProducer(null);
            Message m = sess.createTextMessage("Kippers");
            producerAnon.send(dest, m);
            if (transacted)
            {
               sess.commit();
            }
         }
         catch (JMSSecurityException e)
         {
            log.trace("Can't write to destination using named producer", e);
            anonSucceeded = false;
         }

         if (namedSucceeded || anonSucceeded)
         {
            if (dest instanceof Queue)
            {
               String destName = ((Queue)dest).getQueueName();
               removeAllMessages(destName, true, 0);
            }
         }

         log.trace("namedSucceeded:" + namedSucceeded + ", anonSucceeded:" + anonSucceeded);
         return namedSucceeded || anonSucceeded;
      }
      finally
      {
         sess.close();
      }

   }

   private boolean canCreateDurableSub(Connection conn, Topic topic, String subName) throws Exception
   {

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      try
      {
         MessageConsumer cons = sess.createDurableSubscriber(topic, subName);
         cons.close();
         sess.unsubscribe(subName);
         log.trace("Successfully created and unsubscribed subscription");
         return true;
      }
      catch (JMSSecurityException e)
      {
         log.trace("Can't create durable sub", e);
         return false;
      }
      finally
      {
         sess.close();
      }
   }

   private void testSecurityForTemporaryDestination(boolean isQueue) throws Exception
   {
      Destination dest = isQueue ? (Destination) queue1 : topic1;

      Connection conn = cf.createConnection("guest", "guest");
      try
      {
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination temporaryDestination = isQueue
            ? (Destination) session.createTemporaryQueue()
            : session.createTemporaryTopic();
         Message message = session.createMessage();
         message.setJMSReplyTo(temporaryDestination);
         MessageProducer producer = session.createProducer(dest);
         
         MessageConsumer tmpConsumer = session.createConsumer(temporaryDestination);
         conn.start();
         
         Connection conn2 = cf.createConnection("john", "needle");
         try
         {
            Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(dest);
            conn.start();
            
            producer.send(message);

            Message in = consumer.receive(1000L);
            assertNotNull(in);
            
            Message out = session2.createMessage();
            MessageProducer replyProducer = session2.createProducer(in.getJMSReplyTo());
            replyProducer.send(out);
         }
         finally
         {
            conn2.close();
         }
         
         Message reply = tmpConsumer.receive(1000L);
         assertNotNull(reply);
      }
      finally
      {
         conn.close();
      }
   }

   /**
    * This Validate sending messages on an Queue where the user don't have write authorization
    * @throws Exception
    */
   public void testSecurityOnXA() throws Exception
   {
      XAConnection xaconn = null;

      try
      {
         XAConnectionFactory xacf = (XAConnectionFactory)cf;

         xaconn = xacf.createXAConnection("nobody", "nobody");

         XASession xasession = xaconn.createXASession();

         MessagingXid xid = new MessagingXid(new byte[]{1}, 1, new byte[]{1});

         XAResource resource = xasession.getXAResource();

         resource.start(xid, XAResource.TMNOFLAGS);

         MessageProducer producer = xasession.createProducer(queue1);


         for (int i=0;i<10;i++)
         {
            producer.send(xasession.createTextMessage("Test " + i));
         }

         try
         {
            resource.end(xid, XAResource.TMSUCCESS);
            resource.prepare(xid);
            fail("Didn't throw expected exception!");
         }
         catch (MessagingXAException expected)
         {
         }
      }
      finally
      {
         try
         {
            if (xaconn != null)
            {
               xaconn.close();
            }
            ServerManagement.undeployQueue("MyQueue2");
         }
         catch (Throwable ignored)
         {
         }
      }
   }




   
   // Inner classes -------------------------------------------------

}


