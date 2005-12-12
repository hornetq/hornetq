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

import java.io.StringReader;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

/**
 * 
 * Test JMS Security.
 * 
 * This test must be run with the Test security config. on the server
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Much of the basic idea of the tests come from SecurityUnitTestCase.java in JBossMQ by:
 * @author <a href="pra@tim.se">Peter Antman</a>
 * 
 * In order for this test to run you must ensure:
 * 
 * A JBoss instance is running at localhost
 * jboss-messaging.sar has been deployed to JBoss
 * login-config.xml should have an application-policy as follows:
 * 
 *   <application-policy name="messaging">
 *    <authentication>
 *     <login-module code="org.jboss.security.auth.spi.DatabaseServerLoginModule"
 *       flag="required">
 *       <module-option name="unauthenticatedIdentity">guest</module-option>
 *       <module-option name="dsJndiName">java:/DefaultDS</module-option>
 *        <module-option name="principalsQuery">SELECT PASSWD FROM JMS_USERS WHERE USERID=?</module-option>
 *       <module-option name="rolesQuery">SELECT ROLEID, 'Roles' FROM JMS_ROLES WHERE USERID=?</module-option>
 *     </login-module>
 *    </authentication>
 *   </application-policy>
 *   
 *   
 * 
 * 
 * @version <tt>$Revision$</tt>
 *
 */
public class SecurityTest extends MessagingTestCase
{
   protected Logger log = Logger.getLogger(getClass());
   
   protected static final String TEST_QUEUE = "queue/testQueue";
   protected static final String TEST_TOPIC = "topic/testTopic";
   protected static final String SECURED_TOPIC = "topic/securedTopic";
   protected static final String UNSECURED_TOPIC = "topic/unsecuredTopic";
   
   protected ConnectionFactory cf;
   protected Queue testQueue;
   protected Topic testTopic;
   protected Topic securedTopic;
   protected Topic unsecuredTopic;
   
   protected Element oldDefaultConfig;
   
   // Constructors --------------------------------------------------
   
   public SecurityTest(String name)
   {
      super(name);
   }
   
   // TestCase overrides -------------------------------------------
   
   protected void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
      
      setupDestinations();
      
      final String defaultSecurityConfig = 
         "<security><role name=\"def\" read=\"true\" write=\"true\" create=\"true\"/></security>";
      oldDefaultConfig = ServerManagement.getDefaultSecurityConfig();
      ServerManagement.setDefaultSecurityConfig(toElement(defaultSecurityConfig));
 
            
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      testQueue = (Queue)ic.lookup("/queue/testQueue");
      testTopic = (Topic)ic.lookup("/topic/testTopic");
      securedTopic = (Topic)ic.lookup("/topic/securedTopic");
      unsecuredTopic = (Topic)ic.lookup("/topic/unsecuredTopic");
      
      drainDestination(cf, testQueue);
            
     
   }
   
   protected void tearDown() throws Exception
   {      
      ServerManagement.setDefaultSecurityConfig(oldDefaultConfig);
      ServerManagement.undeployQueue("testQueue");
      ServerManagement.undeployQueue("testTopic");
      ServerManagement.undeployTopic("securedTopic");
      ServerManagement.undeployTopic("unsecuredTopic");
      //ServerManagement.deInit();
   }
   
   
   // Constructors --------------------------------------------------
   
   
   // Public --------------------------------------------------------
   
   
   
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
         log.trace("Can't read destination");
         return false;
      }     
   }
   
   private boolean canWriteDestination(Connection conn, Destination dest) throws Exception
   {
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      boolean namedSucceeded = true;
      try
      {
         sess.createProducer(dest);         
      }
      catch (JMSSecurityException e)
      {
         log.trace("Can't write to destination using named producer");
         namedSucceeded = false;
      }
      
      boolean anonSucceeded = true;
      try
      {         
         MessageProducer producerAnon = sess.createProducer(null);
         Message m = sess.createTextMessage("Kippers");
         producerAnon.send(dest, m);
      }
      catch (JMSSecurityException e)
      {
         log.trace("Can't write to destination using named producer");
         anonSucceeded = false;
      }
      
      log.trace("namedSucceeded:" + namedSucceeded + ", anonSucceeded:" + anonSucceeded);
      return namedSucceeded || anonSucceeded;
      
   }
   
   private boolean canCreateDurableSub(Connection conn, Topic topic, String subName) throws Exception
   {
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         sess.createDurableSubscriber(topic, subName);
         sess.unsubscribe(subName);
         log.trace("Successfully created and unsubscribed subscription");
         return true;
      }
      catch (JMSSecurityException e)
      {
         log.trace("Can't create durable sub");
         return false;
      }    
   }
   
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
   
   
   
   /*
    * user/pwd with preconfigured clientID, should return preconf
    */
   /*
    
    
    This test will not work until client id is automatically preconfigured into
    connection for specific user
    
    public void testPreConfClientID() throws Exception
    {
    Connection conn = null;
    try
    {
    conn = cf.createConnection("john", "needle");
    String clientID = conn.getClientID();
    assertEquals("Invalid ClientID", "DurableSubscriberExample", clientID);
    }
    finally
    {
    if (conn != null) conn.close();
    }
    }
    */
   /*
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
   
   /*
    * Try setting client ID on preconfigured connection - should throw exception
    */
   /*
    * 
    
    
    This test will not work until client id is automatically preconfigured into
    connection for specific user
    
    public void testSetClientIDPreConf() throws Exception
    {
    Connection conn = null;
    try
    {
    conn = cf.createConnection("john", "needle");
    conn.setClientID("myID");
    fail();
    }
    catch (InvalidClientIDException e)
    {
    //Expected
     }
     finally
     {
     if (conn != null) conn.close();
     }
     }
     */
   
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
   
   
   /* Authorization tests */
   
   
   /*
    * Test valid topic publisher
    */
   public void testValidTopicPublisher() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");        
         assertTrue(this.canWriteDestination(conn, testTopic));
      }        
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   
   /*
    * Test invalid topic publisher
    */
   public void testInvalidTopicPublisher() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");        
         assertFalse(this.canWriteDestination(conn, testTopic));
      }    
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   
   /*
    * Test valid topic subscriber
    */
   public void testValidTopicSubscriber() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");        
         assertTrue(this.canReadDestination(conn, testTopic));
      }    
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   /*
    * Test invalid topic subscriber
    */
   public void testInvalidTopicSubscriber() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");        
         assertFalse(this.canReadDestination(conn, testTopic));
      }    
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   
   /*
    * Test valid queue browser
    */
   public void testValidQueueBrowser() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");        
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         sess.createBrowser(testQueue);
      }    
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   /*
    * Test invalid queue browser
    */
   public void testInvalidQueueBrowser() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");        
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         sess.createBrowser(testQueue);
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
   
   
   
   /*
    * Test valid queue sender
    */
   public void testValidQueueSender() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");        
         assertTrue(this.canWriteDestination(conn, testQueue));
      }        
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   
   /*
    * Test invalid queue sender
    */
   public void testInvalidQueueSender() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");        
         assertFalse(this.canWriteDestination(conn, testQueue));
      }    
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   
   
   /*
    * Test valid queue receiver
    */
   public void testValidQueueReceiver() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("john", "needle");        
         assertTrue(this.canReadDestination(conn, testQueue));
      }    
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   /*
    * Test invalid queue receiver
    */
   public void testInvalidQueueReceiver() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection("nobody", "nobody");        
         assertFalse(this.canReadDestination(conn, testQueue));
      }    
      finally
      {
         if (conn != null) conn.close();
      }
   }
   
   
   /*
    * Test valid durable subscription creation for connection preconfigured with client id
    */
   
   /*
    
    This test will not work until client id is automatically preconfigured into
    connection for specific user
    
    public void testValidDurableSubscriptionCreationPreConf() throws Exception
    {
    Connection conn = null;
    try
    {
    conn = cf.createConnection("john", "needle");        
    assertTrue(this.canCreateDurableSub(conn, testTopic, "sub2"));
    }          
    finally
    {
    if (conn != null) conn.close();
    }
    }
    
    */
   
   /*
    * Test invalid durable subscription creation for connection preconfigured with client id
    */
   
   
   /*
    
    This test will not work until client id is automatically preconfigured into
    connection for specific user
    public void testInvalidDurableSubscriptionCreationPreConf() throws Exception
    {
    Connection conn = null;
    try
    {
    conn = cf.createConnection("john", "needle");        
    assertFalse(this.canCreateDurableSub(conn, securedTopic, "sub3"));
    }    
    finally
    {
    if (conn != null) conn.close();
    }
    }
    
    */
   
   
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
         assertTrue(this.canCreateDurableSub(conn, testTopic, "sub4"));
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
         assertFalse(this.canCreateDurableSub(conn, securedTopic, "sub5"));
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
         assertTrue(this.canReadDestination(conn, unsecuredTopic));
         assertTrue(this.canWriteDestination(conn, unsecuredTopic));
         assertTrue(this.canCreateDurableSub(conn, unsecuredTopic, "subxyz"));
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
         assertFalse(this.canReadDestination(conn, unsecuredTopic));
         assertFalse(this.canWriteDestination(conn, unsecuredTopic));
         assertFalse(this.canCreateDurableSub(conn, unsecuredTopic, "subabc"));
      }          
      finally
      {
         if (conn != null) conn.close();
      }
   }
  
   
   /* Setup all the destinations needed for the tests.
    * We need the following destinations:
    * 
    *
    * 
    */
   private void setupDestinations() throws Exception
   {
      ServerManagement.undeployQueue("testQueue");
      ServerManagement.deployQueue("testQueue");
            
      final String testQueueConf = 
         "<security>" +
            "<role name=\"guest\" read=\"true\" write=\"true\"/>" +
            "<role name=\"publisher\" read=\"true\" write=\"true\" create=\"false\"/>" +
            "<role name=\"noacc\" read=\"false\" write=\"false\" create=\"false\"/>" +
         "</security>";
                     
      ServerManagement.setSecurityConfig("testQueue", toElement(testQueueConf));
      
      ServerManagement.undeployTopic("testTopic");
      ServerManagement.deployTopic("testTopic");
            
      final String testTopicConf = 
         "<security>" +
            "<role name=\"guest\" read=\"true\" write=\"true\"/>" +
            "<role name=\"publisher\" read=\"true\" write=\"true\" create=\"false\"/>" +
            "<role name=\"durpublisher\" read=\"true\" write=\"true\" create=\"true\"/>" +
         "</security>";
                     
      ServerManagement.setSecurityConfig("testTopic", toElement(testTopicConf));
      
      ServerManagement.undeployTopic("securedTopic");
      ServerManagement.deployTopic("securedTopic");
      
      final String testSecuredTopicConf = 
         "<security>" +         
            "<role name=\"publisher\" read=\"true\" write=\"true\" create=\"false\"/>" +
         "</security>";
                     
      ServerManagement.setSecurityConfig("testSecuredTopic", toElement(testSecuredTopicConf));
      
      ServerManagement.undeployTopic("unsecuredTopic");
      ServerManagement.deployTopic("unsecuredTopic");
      
   }
   
   private Element toElement(String s)
      throws Exception
   {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder parser = factory.newDocumentBuilder();
      Document doc = parser.parse(new InputSource(new StringReader(s)));
      return doc.getDocumentElement();
   }
   
}


