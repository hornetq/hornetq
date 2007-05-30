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
package org.jboss.test.messaging.jms.server.destination.base;

import java.util.List;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.jms.server.destination.ManagedDestination;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.messaging.util.XMLUtil;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.w3c.dom.Element;

/**
 * Exercises a destinatio's management interface after deployment.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DestinationManagementTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public DestinationManagementTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");   
      
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   
   public void testDeployDestinationAdministratively() throws Exception
   {
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@Service\" " +
         "       name=\"somedomain:service=@TOREPLACE@,name=Kirkwood\"" +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "</mbean>";

      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);

      assertEquals("Kirkwood", ServerManagement.getAttribute(destObjectName, "Name"));

      String jndiName = (isQueue() ? "/queue" : "/topic") + "/Kirkwood";
      String s = (String)ServerManagement.getAttribute(destObjectName, "JNDIName");
      assertEquals(jndiName, s);
  
      Set destinations = (Set)ServerManagement.getAttribute(serverPeerObjectName, "Destinations");

      assertEquals(1, destinations.size());

      if (isQueue())
      {
         Queue q = (Queue)destinations.iterator().next();
         assertEquals("Kirkwood", q.getQueueName());
      }
      else
      {
         Topic t = (Topic)destinations.iterator().next();
         assertEquals("Kirkwood", t.getTopicName());
      }

      assertEquals(serverPeerObjectName,
                   ServerManagement.getAttribute(destObjectName, "ServerPeer"));

      // try to change it
      ServerManagement.setAttribute(destObjectName, "ServerPeer",
                                    "theresnosuchdomain:service=TheresNoSuchService");

      assertEquals(serverPeerObjectName,
                   ServerManagement.getAttribute(destObjectName, "ServerPeer"));

      undeployDestination((String)ServerManagement.getAttribute(destObjectName, "Name"));
   }
   
   public void testDeployDestinationAdministrativelyWithParams() throws Exception
   {
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();
      
      int fullSize = 77777;
      
      int pageSize = 1234;
      
      int downCacheSize = 789;

      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@Service\" " +
         "       name=\"somedomain:service=@TOREPLACE@,name=Kirkwood\"" +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "    <attribute name=\"FullSize\">" + fullSize + "</attribute>" +
         "    <attribute name=\"PageSize\">" + pageSize + "</attribute>" +
         "    <attribute name=\"DownCacheSize\">" + downCacheSize + "</attribute>" +
         "</mbean>";

      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);

      assertEquals("Kirkwood", ServerManagement.getAttribute(destObjectName, "Name"));

      String jndiName = (isQueue() ? "/queue" : "/topic") + "/Kirkwood";
      String s = (String)ServerManagement.getAttribute(destObjectName, "JNDIName");
      assertEquals(jndiName, s);
  
      Set destinations = (Set)ServerManagement.getAttribute(serverPeerObjectName, "Destinations");

      assertEquals(1, destinations.size());

      if (isQueue())
      {
         Queue q = (Queue)destinations.iterator().next();
         assertEquals("Kirkwood", q.getQueueName());
         
      }
      else
      {
         Topic t = (Topic)destinations.iterator().next();
         assertEquals("Kirkwood", t.getTopicName());
      }

      assertEquals(serverPeerObjectName,
                   ServerManagement.getAttribute(destObjectName, "ServerPeer"));
      
      assertEquals(new Integer(fullSize), ServerManagement.getAttribute(destObjectName, "FullSize"));
      assertEquals(new Integer(pageSize), ServerManagement.getAttribute(destObjectName, "PageSize"));
      assertEquals(new Integer(downCacheSize), ServerManagement.getAttribute(destObjectName, "DownCacheSize"));

      // try to change it
      ServerManagement.setAttribute(destObjectName, "ServerPeer",
                                    "theresnosuchdomain:service=TheresNoSuchService");

      assertEquals(serverPeerObjectName,
                   ServerManagement.getAttribute(destObjectName, "ServerPeer"));

      undeployDestination((String)ServerManagement.getAttribute(destObjectName, "Name"));
   }

   public void testDefaultSecurityConfiguration() throws Exception
   {
      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@Service\" " +
         "       name=\"somedomain:service=@TOREPLACE@,name=DefaultSecurity\"" +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "</mbean>";

      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);

      assertNull(ServerManagement.getAttribute(destObjectName, "SecurityConfig"));

      undeployDestination((String)ServerManagement.getAttribute(destObjectName, "Name"));
   }

   public void testSecurityConfigurationManagement() throws Exception
   {
      String securityConfig =
         "        <security>\n" +
         "           <role name=\"guest\" read=\"true\" write=\"true\"/>\n" +
         "           <role name=\"publisher\" read=\"true\" write=\"true\" create=\"false\"/>\n" +
         "           <role name=\"durpublisher\" read=\"true\" write=\"true\" create=\"true\"/>\n" +
         "        </security>";

      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@Service\"\n" +
         "       name=\"somedomain:service=@TOREPLACE@,name=DefaultSecurity\"\n" +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">\n" +
         "       <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>\n" +
         "       <attribute name=\"SecurityConfig\">\n" +
                 securityConfig +
         "       </attribute> \n" +
         "</mbean>";


      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);

      Element security = (Element)ServerManagement.getAttribute(destObjectName, "SecurityConfig");

      XMLUtil.assertEquivalent(XMLUtil.stringToElement(securityConfig), security);

      undeployDestination((String)ServerManagement.getAttribute(destObjectName, "Name"));
   }

   public void testArbitraryJNDIName() throws Exception
   {
      String testJNDIName = "/a/totally/arbitrary/jndi/name/thisisthequeue";

      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@Service\" " +
         "       name=\"somedomain:service=@TOREPLACE@,name=Kirkwood\"" +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "    <attribute name=\"JNDIName\">" + testJNDIName + "</attribute>" +
         "</mbean>";

      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);

      assertEquals("Kirkwood", ServerManagement.getAttribute(destObjectName, "Name"));
      assertEquals(testJNDIName, ServerManagement.getAttribute(destObjectName, "JNDIName"));

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      Destination d = (Destination)ic.lookup(testJNDIName);

      if (isQueue())
      {
         Queue q = (Queue)d;
         assertEquals("Kirkwood", q.getQueueName());
      }
      else
      {
         Topic t = (Topic)d;
         assertEquals("Kirkwood", t.getTopicName());
      }

      ic.close();

      // try to change the JNDI name after initialization

      ServerManagement.setAttribute(destObjectName, "JNDIName", "total/junk");
      assertEquals(testJNDIName, ServerManagement.getAttribute(destObjectName, "JNDIName"));

      undeployDestination((String)ServerManagement.getAttribute(destObjectName, "Name"));
   }


   public void testDeployDestinationProgrammatically() throws Exception
   {
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

      String destinationType = isQueue() ? "Queue" : "Topic";
      String deployMethod = "deploy" + destinationType;
      String undeployMethod = "undeploy" + destinationType;
      String destinationName = "BlahBlah";
      String expectedJNDIName = (isQueue() ? "/queue/" : "/topic/") + destinationName;
      ObjectName destObjectName = new ObjectName("jboss.messaging.destination:service=" +
                                                 destinationType +",name=" + destinationName);

      // deploy it

      String jndiName = (String)ServerManagement.
         invoke(serverPeerObjectName, deployMethod,
                new Object[] { destinationName, null },
                new String[] { "java.lang.String", "java.lang.String" });

      assertEquals(expectedJNDIName, jndiName);

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      if (isQueue())
      {
         Queue q = (Queue)ic.lookup(jndiName);
         assertEquals(destinationName, q.getQueueName());
      }
      else
      {
         Topic t = (Topic)ic.lookup(jndiName);
         assertEquals(destinationName, t.getTopicName());
      }

      assertEquals(destinationName, ServerManagement.getAttribute(destObjectName, "Name"));
      assertEquals(expectedJNDIName,
                   (String)ServerManagement.getAttribute(destObjectName, "JNDIName"));

      // undeploy it

      Boolean b = (Boolean)ServerManagement.invoke(serverPeerObjectName, undeployMethod,
                                                   new Object[] { destinationName },
                                                   new String[] { "java.lang.String" });

      assertTrue(b.booleanValue());

      try
      {
         ic.lookup(expectedJNDIName);
         fail("should throw exception");
      }
      catch(NamingException e)
      {
         // OK
      }

      Set set = ServerManagement.query(destObjectName);
      assertTrue(set.isEmpty());
 
      set = (Set)ServerManagement.getAttribute(serverPeerObjectName, "Destinations");


      assertTrue(set.isEmpty());

      ic.close();

   }
   
   public void testDeployDestinationProgrammaticallyWithParams() throws Exception
   {
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

      String destinationType = isQueue() ? "Queue" : "Topic";
      String deployMethod = "deploy" + destinationType;
      String undeployMethod = "undeploy" + destinationType;
      String destinationName = "BlahBlah";
      String expectedJNDIName = (isQueue() ? "/queue/" : "/topic/") + destinationName;
      ObjectName destObjectName = new ObjectName("jboss.messaging.destination:service=" +
                                                 destinationType +",name=" + destinationName);

      int fullSize = 6565454;
      
      int pageSize = 6565;
      
      int downCacheSize = 123;
      
      // deploy it

      String jndiName = (String)ServerManagement.
         invoke(serverPeerObjectName, deployMethod,
                new Object[] { destinationName, null, new Integer(fullSize), new Integer(pageSize), new Integer(downCacheSize)},
                new String[] { "java.lang.String", "java.lang.String", "int", "int", "int"});

      assertEquals(expectedJNDIName, jndiName);

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      if (isQueue())
      {
         Queue q = (Queue)ic.lookup(jndiName);
         assertEquals(destinationName, q.getQueueName());
      }
      else
      {
         Topic t = (Topic)ic.lookup(jndiName);
         assertEquals(destinationName, t.getTopicName());
      }

      assertEquals(destinationName, ServerManagement.getAttribute(destObjectName, "Name"));
      assertEquals(expectedJNDIName,
                   (String)ServerManagement.getAttribute(destObjectName, "JNDIName"));
      
      assertEquals(new Integer(fullSize), ServerManagement.getAttribute(destObjectName, "FullSize"));
      assertEquals(new Integer(pageSize), ServerManagement.getAttribute(destObjectName, "PageSize"));
      assertEquals(new Integer(downCacheSize), ServerManagement.getAttribute(destObjectName, "DownCacheSize"));


      // undeploy it

      Boolean b = (Boolean)ServerManagement.invoke(serverPeerObjectName, undeployMethod,
                                                   new Object[] { destinationName },
                                                   new String[] { "java.lang.String" });

      assertTrue(b.booleanValue());

      try
      {
         ic.lookup(expectedJNDIName);
         fail("should throw exception");
      }
      catch(NamingException e)
      {
         // OK
      }

      Set set = ServerManagement.query(destObjectName);
      assertTrue(set.isEmpty());
 
      set = (Set)ServerManagement.getAttribute(serverPeerObjectName, "Destinations");


      assertTrue(set.isEmpty());

      ic.close();

   }
   
   public void testDestroyDestinationProgrammatically() throws Exception
   {
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

      String destinationType = isQueue() ? "Queue" : "Topic";
      String deployMethod = "deploy" + destinationType;
      String destroyMethod = "destroy" + destinationType;
      String destinationName = "BlahBlah";
      String expectedJNDIName = (isQueue() ? "/queue/" : "/topic/") + destinationName;
      ObjectName destObjectName = new ObjectName("jboss.messaging.destination:service=" +
                                                 destinationType +",name=" + destinationName);

      // deploy it

      String jndiName = (String)ServerManagement.
         invoke(serverPeerObjectName, deployMethod,
                new Object[] { destinationName, null },
                new String[] { "java.lang.String", "java.lang.String" });

      assertEquals(expectedJNDIName, jndiName);

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      if (isQueue())
      {
         Queue q = (Queue)ic.lookup(jndiName);
         assertEquals(destinationName, q.getQueueName());
      }
      else
      {
         Topic t = (Topic)ic.lookup(jndiName);
         assertEquals(destinationName, t.getTopicName());
      }

      assertEquals(destinationName, ServerManagement.getAttribute(destObjectName, "Name"));
      assertEquals(expectedJNDIName,
                   (String)ServerManagement.getAttribute(destObjectName, "JNDIName"));
      
      ConnectionFactory cf = (ConnectionFactory)this.initialContext.lookup("/ConnectionFactory");
      
      Connection conn = cf.createConnection();
      
      conn.setClientID("wibble456");
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      conn.start();
      
      Destination dest;
      
      if (!isQueue())
      {
         dest = (Destination)ic.lookup("/topic/" + destinationName);
         
         //Create a durable sub
         sess.createDurableSubscriber((Topic)dest, "sub1");
      }
      else
      {
         dest = (Destination)ic.lookup("/queue/" + destinationName);
      }
      
      //Send a message
      
      TextMessage tm = sess.createTextMessage("uuuuuuuuurrrrrrrrgggggggggghhhhh");
      
      MessageProducer prod = sess.createProducer(dest);
      
      prod.send(tm);
      
      conn.close();
            
      // destroy it

      Boolean b = (Boolean)ServerManagement.invoke(serverPeerObjectName, destroyMethod,
                                                   new Object[] { destinationName },
                                                   new String[] { "java.lang.String" });

      assertTrue(b.booleanValue());

      try
      {
         ic.lookup(expectedJNDIName);
         fail("should throw exception");
      }
      catch(NamingException e)
      {
         // OK
      }
      
      //Make sure no bindings exist
      //ServerManagement.gets

      Set set = ServerManagement.query(destObjectName);
      assertTrue(set.isEmpty());
 
      set = (Set)ServerManagement.getAttribute(serverPeerObjectName, "Destinations");


      assertTrue(set.isEmpty());
      
      // Deploy it again
      jndiName = (String)ServerManagement.
      invoke(serverPeerObjectName, deployMethod,
             new Object[] { destinationName, null },
             new String[] { "java.lang.String", "java.lang.String" });
      
      conn = cf.createConnection();
      
      conn.setClientID("wibble456");
      
      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      conn.start();
      
      MessageConsumer cons;
      
      if (isQueue())
      {
         cons = sess.createConsumer((Queue)dest);
      }
      else
      {
         cons = sess.createDurableSubscriber((Topic)dest, "sub1");
      }
      
      Message m = cons.receive(2000);
      
      assertNull(m);
      
      conn.close();
      

      ic.close();

   }

   public void testDestroyNonProgrammaticDestination() throws Exception
   {
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

      String destinationType = isQueue() ? "Queue" : "Topic";
      String destroyMethod = "destroy" + destinationType;
      String destinationName = "XXX";

      // deploy "classically"

      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@Service\" " +
         "       name=\"jboss.messaging.destination:service=@TOREPLACE@,name=" + destinationName + "\" " +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">" + serverPeerObjectName + "</depends>" +
         "</mbean>";

      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);

      assertEquals(destinationName, ServerManagement.getAttribute(destObjectName, "Name"));

      // try to undeploy programatically

      Boolean b = (Boolean)ServerManagement.invoke(serverPeerObjectName, destroyMethod,
                                                   new Object[] { destinationName },
                                                   new String[] { "java.lang.String" });

      assertFalse(b.booleanValue());
   }

   public void testPageableChannelAttributes() throws Exception
   {
   	if (ServerManagement.isRemote())
   	{
   		//This test can't be run in a remote configuration
   		return;   	
   	}
   	
      int fullSize = 7777;
      int pageSize = 234;
      int downCacheSize = 56;

      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@Service\" " +
         "       name=\"somedomain:service=@TOREPLACE@,name=PageableAttributes\"" +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "    <attribute name=\"FullSize\">" + fullSize + "</attribute>" +
         "    <attribute name=\"PageSize\">" + pageSize + "</attribute>" +
         "    <attribute name=\"DownCacheSize\">" + downCacheSize + "</attribute>" +
         "</mbean>";

      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);
      
      // Test the default values
      assertEquals(new Integer(fullSize), ServerManagement.getAttribute(destObjectName, "FullSize"));
      assertEquals(new Integer(pageSize), ServerManagement.getAttribute(destObjectName, "PageSize"));
      assertEquals(new Integer(downCacheSize), ServerManagement.getAttribute(destObjectName, "DownCacheSize"));

      JBossDestination jbd2;
      
      if (isQueue())
      {
         jbd2 = new JBossQueue("PageableAttributes");
      }
      else
      {
         jbd2 = new JBossTopic("PageableAttributes");
      }
      
      ManagedDestination mDest = ServerManagement.getDestinationManager().getDestination(jbd2.getName(), jbd2.isQueue());
       
      assertEquals(fullSize, mDest.getFullSize());
      assertEquals(pageSize, mDest.getPageSize());
      assertEquals(downCacheSize, mDest.getDownCacheSize());
      
      // Try to change the values when destination lives, no effect
      ServerManagement.setAttribute(destObjectName, "FullSize", "1111");
      assertEquals(new Integer(fullSize), ServerManagement.getAttribute(destObjectName, "FullSize"));
      ServerManagement.setAttribute(destObjectName, "PageSize", "222");
      assertEquals(new Integer(pageSize), ServerManagement.getAttribute(destObjectName, "PageSize"));
      ServerManagement.setAttribute(destObjectName, "DownCacheSize", "33");
      assertEquals(new Integer(downCacheSize), ServerManagement.getAttribute(destObjectName, "DownCacheSize"));
      
      // Stop the destination and change the value then test them from MBean
      ServerManagement.invoke(destObjectName, "stop", null, null);
      ServerManagement.setAttribute(destObjectName, "DownCacheSize", "9999");
      assertEquals(new Integer(9999), ServerManagement.getAttribute(destObjectName, "DownCacheSize"));
      ServerManagement.setAttribute(destObjectName, "PageSize", "20019");
      assertEquals(new Integer(20019), ServerManagement.getAttribute(destObjectName, "PageSize"));
      ServerManagement.setAttribute(destObjectName, "FullSize", "751119");
      assertEquals(new Integer(751119), ServerManagement.getAttribute(destObjectName, "FullSize"));
 
      // Start the service again and test the value from core destination
      ServerManagement.invoke(destObjectName, "start", null, null);
      
      mDest = ServerManagement.getDestinationManager().getDestination(jbd2.getName(), jbd2.isQueue());
      
      // Must get the new core destination!      
      assertEquals(751119, mDest.getFullSize());
      assertEquals(20019, mDest.getPageSize());
      assertEquals(9999, mDest.getDownCacheSize());

      undeployDestination("PageableAttributes");
   }
   
   public void testGetSetMessageCounterHistoryDayLimit() throws Exception
   {
   	if (ServerManagement.isRemote())
   	{
   		//This test can't be run in a remote configuration since MessageCounter is not serializable
   		return;   	
   	}
   	
      int defaultLimit = 12;
      
      ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "DefaultMessageCounterHistoryDayLimit", String.valueOf(defaultLimit));
      
      ServerManagement.deployQueue("testQueue");
      
      ServerManagement.deployTopic("testTopic");
      
      Connection conn = null;
      
      try
      {         
         initialContext.lookup("/queue/testQueue");
         
         Topic testTopic = (Topic)initialContext.lookup("/topic/testTopic");
               
         String queueON = "jboss.messaging.destination:service=Queue,name=testQueue";
         
         String topicON = "jboss.messaging.destination:service=Topic,name=testTopic";
            
         MessageCounter queueCounter = (MessageCounter)ServerManagement.getAttribute(new ObjectName(queueON), "MessageCounter");
         
         assertEquals(defaultLimit, queueCounter.getHistoryLimit());
         
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         
         conn = cf.createConnection();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         sess.createConsumer(testTopic);
         
         List counters = (List)ServerManagement.getAttribute(new ObjectName(topicON), "MessageCounters");
         
         assertNotNull(counters);
         
         assertEquals(1, counters.size());
         
         MessageCounter subCounter = (MessageCounter)counters.get(0);
         
         assertEquals(defaultLimit, subCounter.getHistoryLimit());
         
         int overrideLimit = 777;
         
         ServerManagement.setAttribute(new ObjectName(queueON), "MessageCounterHistoryDayLimit", String.valueOf(overrideLimit));
         
         ServerManagement.setAttribute(new ObjectName(topicON), "MessageCounterHistoryDayLimit", String.valueOf(overrideLimit));
         
         assertEquals(overrideLimit, queueCounter.getHistoryLimit());
         
         assertEquals(overrideLimit, subCounter.getHistoryLimit());
      }
      finally
      {
         if (conn != null) conn.close();
         
         ServerManagement.undeployQueue("testQueue");
         
         ServerManagement.undeployTopic("testTopic");
      }                    
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract boolean isQueue();
   
   protected ObjectName deploy(String destConfig) throws Exception
   {
      ObjectName on = ServerManagement.deploy(destConfig);

      ServerManagement.invoke(on, "create", new Object[0], new String[0]);
      ServerManagement.invoke(on, "start", new Object[0], new String[0]);

      return on;
   }
   
   protected void undeployDestination(String name) throws Exception
   {
      if (isQueue())
      {
         ServerManagement.undeployQueue(name);
      }
      else
      {
         ServerManagement.undeployTopic(name);
      }
   }

   // Private -------------------------------------------------------

   

   private String adjustConfiguration(String config)
   {
      return config.replaceAll("@TOREPLACE@", isQueue() ? "Queue" : "Topic");
   }

   

   // Inner classes -------------------------------------------------
}
