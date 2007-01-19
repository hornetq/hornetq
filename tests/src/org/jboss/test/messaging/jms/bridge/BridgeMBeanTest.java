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
package org.jboss.test.messaging.jms.bridge;

import java.io.ByteArrayOutputStream;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.server.bridge.Bridge;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A BridgeMBeanTest
 * 
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeMBeanTest extends BridgeTestBase
{
   private static final Logger log = Logger.getLogger(BridgeMBeanTest.class);
   
   
   public BridgeMBeanTest(String name)
   {
      super(name);
   }
   
   protected void setUp() throws Exception
   {
      nodeCount = 3;
      
      super.setUp();
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   public void testStopStartPauseResume() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      
      ServerManagement.deployQueue("sourceQueue", 1);
      ServerManagement.deployQueue("targetQueue", 2);
      
      Properties props1 = new Properties();
      props1.putAll(ServerManagement.getJNDIEnvironment(1));
      
      Properties props2 = new Properties();
      props2.putAll(ServerManagement.getJNDIEnvironment(2));
      
      String sprops1 = tableToString(props1);
      
      String sprops2 = tableToString(props2);
      
      ObjectName on = deployBridge(0, "Bridge1", "/XAConnectionFactory", "/XAConnectionFactory",
                                   "/queue/sourceQueue", "/queue/targetQueue",
                                   null, null, null, null,
                                   Bridge.QOS_AT_MOST_ONCE, null, 1,
                                   -1, null, null, 5000, -1,
                                   sprops1, sprops2);
      
      ServerManagement.getServer(0).invoke(on, "create", new Object[0], new String[0]);
      
      Connection connSource = null;
      
      Connection connTarget = null;
      
      InitialContext icSource = new InitialContext(props1);
      InitialContext icTarget = new InitialContext(props2);
      
      try
      {
         ConnectionFactory cf0 = (ConnectionFactory)icSource.lookup("/XAConnectionFactory");
         
         ConnectionFactory cf1 = (ConnectionFactory)icTarget.lookup("/XAConnectionFactory");
         
         Destination destSource = (Destination)icSource.lookup("/queue/sourceQueue");
         
         Destination destTarget = (Destination)icTarget.lookup("/queue/targetQueue");
         
         connSource = cf0.createConnection();
         
         connTarget = cf1.createConnection();
         
         connTarget.start();
         
         connSource.start();
         
         final int NUM_MESSAGES = 50;
         
         Session sessSource = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSource.createProducer(destSource);
         
         Session sessTarget = connTarget.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessTarget.createConsumer(destTarget);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSource.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         //It's stopped so no messages should be received
         
         Message m = cons.receive(2000);
         
         assertNull(m);
         
         //Start it
         
         ServerManagement.getServer(0).invoke(on, "start", new Object[0], new String[0]);
         
         //Now should receive the messages
                  
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(2000);
            
            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         m = cons.receive(2000);
         
         assertNull(m);
         
         
         //Send some more
         
         for (int i = NUM_MESSAGES; i < 2 * NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSource.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         //These should be received too
         
         for (int i = NUM_MESSAGES; i < 2 * NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(2000);
            
            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         m = cons.receive(2000);
         
         assertNull(m);
         
         //Pause it
         
         ServerManagement.getServer(0).invoke(on, "pause", new Object[0], new String[0]);
         
         boolean isPaused = ((Boolean)ServerManagement.getAttribute(on, "Paused")).booleanValue();
         
         assertTrue(isPaused);
         
         // Send some more
         
         for (int i = 2 * NUM_MESSAGES; i < 3 * NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSource.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         //These shouldn't be received
         
         m = cons.receive(2000);
         
         assertNull(m);
         
         // Resume
         
         ServerManagement.getServer(0).invoke(on, "resume", new Object[0], new String[0]);
         
         //Now messages should be received
         
         for (int i = 2 * NUM_MESSAGES; i < 3 * NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(2000);
            
            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         m = cons.receive(2000);
         
         assertNull(m);
         
         isPaused = ((Boolean)ServerManagement.getAttribute(on, "Paused")).booleanValue();
         
         assertFalse(isPaused);
         
         //Stop
         
         ServerManagement.getServer(0).invoke(on, "stop", new Object[0], new String[0]);
         
         boolean isStarted = ((Boolean)ServerManagement.getAttribute(on, "Started")).booleanValue();
         
         assertFalse(isStarted);
         
         MessageConsumer cons2 = sessSource.createConsumer(destSource);
         
         m = cons2.receive(2000);
         
         assertNull(m);
         
      }
      finally
      {
         if (connSource != null)
         {
            connSource.close();
         }
         
         if (connTarget != null)
         {
            connTarget.close();
         }
      }
   }
         
   public void testDeploy() throws Exception
   {
      if (!ServerManagement.isRemote())
      {
         return;
      }
      
      ServerManagement.deployQueue("sourceQueue", 1);
      ServerManagement.deployQueue("targetQueue", 2);
      
      try
      {         
         Thread.sleep(5000);
         
         Properties props1 = new Properties();
         props1.putAll(ServerManagement.getJNDIEnvironment(1));
         
         Properties props2 = new Properties();
         props2.putAll(ServerManagement.getJNDIEnvironment(2));
         
         String sprops1 = tableToString(props1);
         
         String sprops2 = tableToString(props2);
         
         ObjectName on = deployBridge(0, "Bridge1", "/XAConnectionFactory", "/XAConnectionFactory",
                                      "/queue/sourceQueue", "/queue/targetQueue",
                                      null, null, null, null,
                                      Bridge.QOS_ONCE_AND_ONLY_ONCE, null, 1,
                                      -1, null, null, 5000, -1,
                                      sprops1, sprops2);
         
         log.trace("Constructed bridge");
         
         ServerManagement.getServer(0).invoke(on, "create", new Object[0], new String[0]);
         
         log.trace("Created bridge");
            
         {
            String cfLookup = (String)ServerManagement.getAttribute(on, "SourceConnectionFactoryLookup");
            assertEquals("/XAConnectionFactory", cfLookup);
            ServerManagement.setAttribute(on, "SourceConnectionFactoryLookup", "/Wibble");
            cfLookup = (String)ServerManagement.getAttribute(on, "SourceConnectionFactoryLookup");
            assertEquals("/Wibble", cfLookup);
            ServerManagement.setAttribute(on, "SourceConnectionFactoryLookup", "/XAConnectionFactory");
         }
              
         {
            String cfLookup = (String)ServerManagement.getAttribute(on, "TargetConnectionFactoryLookup");
            assertEquals("/XAConnectionFactory", cfLookup);
            ServerManagement.setAttribute(on, "TargetConnectionFactoryLookup", "/Wibble");
            cfLookup = (String)ServerManagement.getAttribute(on, "TargetConnectionFactoryLookup");
            assertEquals("/Wibble", cfLookup);
            ServerManagement.setAttribute(on, "TargetConnectionFactoryLookup", "/XAConnectionFactory");
         }
         
         {
            String destLookup = (String)ServerManagement.getAttribute(on, "SourceDestinationLookup");
            assertEquals("/queue/sourceQueue", destLookup);
            ServerManagement.setAttribute(on, "SourceDestinationLookup", "/queue/WibbleQueue");
            destLookup = (String)ServerManagement.getAttribute(on, "SourceDestinationLookup");
            assertEquals("/queue/WibbleQueue", destLookup);
            ServerManagement.setAttribute(on, "SourceDestinationLookup", "/queue/sourceQueue");
         }
         
         {
            String destLookup = (String)ServerManagement.getAttribute(on, "TargetDestinationLookup");
            assertEquals("/queue/targetQueue", destLookup);
            ServerManagement.setAttribute(on, "TargetDestinationLookup", "/queue/WibbleQueue");
            destLookup = (String)ServerManagement.getAttribute(on, "TargetDestinationLookup");
            assertEquals("/queue/WibbleQueue", destLookup);
            ServerManagement.setAttribute(on, "TargetDestinationLookup", "/queue/targetQueue");
         }
         
         {
            String username = (String)ServerManagement.getAttribute(on, "SourceUsername");
            assertEquals(null, username);
            ServerManagement.setAttribute(on, "SourceUsername", "bob");
            username = (String)ServerManagement.getAttribute(on, "SourceUsername");
            assertEquals("bob", username);
            ServerManagement.setAttribute(on, "SourceUsername", null);
         }
         
         {
            String password = (String)ServerManagement.getAttribute(on, "SourcePassword");
            assertEquals(null, password);
            ServerManagement.setAttribute(on, "SourcePassword", "eek");
            password = (String)ServerManagement.getAttribute(on, "SourcePassword");
            assertEquals("eek", password);
            ServerManagement.setAttribute(on, "SourcePassword", null);
         }
         
         {
            String username = (String)ServerManagement.getAttribute(on, "TargetUsername");
            assertEquals(null, username);
            ServerManagement.setAttribute(on, "TargetUsername", "bob");
            username = (String)ServerManagement.getAttribute(on, "TargetUsername");
            assertEquals("bob", username);
            ServerManagement.setAttribute(on, "TargetUsername", null);
         }
         
         {
            String password = (String)ServerManagement.getAttribute(on, "TargetPassword");
            assertEquals(null, password);
            ServerManagement.setAttribute(on, "TargetPassword", "eek");
            password = (String)ServerManagement.getAttribute(on, "TargetPassword");
            assertEquals("eek", password);
            ServerManagement.setAttribute(on, "TargetPassword", null);
         }
         
         {
            Integer qos = (Integer)ServerManagement.getAttribute(on, "QualityOfServiceMode");
            assertEquals(Bridge.QOS_ONCE_AND_ONLY_ONCE, qos.intValue());
            ServerManagement.setAttribute(on, "QualityOfServiceMode", String.valueOf(Bridge.QOS_AT_MOST_ONCE));
            qos = (Integer)ServerManagement.getAttribute(on, "QualityOfServiceMode");
            assertEquals(new Integer(Bridge.QOS_AT_MOST_ONCE), qos);
            ServerManagement.setAttribute(on, "QualityOfServiceMode", String.valueOf(Bridge.QOS_ONCE_AND_ONLY_ONCE));
         }
         
         {
            String selector = (String)ServerManagement.getAttribute(on, "Selector");
            assertEquals(null, selector);
            ServerManagement.setAttribute(on, "Selector", "god='dead'");
            selector = (String)ServerManagement.getAttribute(on, "Selector");
            assertEquals("god='dead'", selector);
            ServerManagement.setAttribute(on, "Selector", null);
         }
         
         {
            Integer maxBatchSize = (Integer)ServerManagement.getAttribute(on, "MaxBatchSize");
            assertEquals(1, maxBatchSize.intValue());
            ServerManagement.setAttribute(on, "MaxBatchSize", "10");
            maxBatchSize = (Integer)ServerManagement.getAttribute(on, "MaxBatchSize");
            assertEquals(10, maxBatchSize.intValue());
            ServerManagement.setAttribute(on, "MaxBatchSize", "1");
         }
         
         {
            Long maxBatchTime = (Long)ServerManagement.getAttribute(on, "MaxBatchTime");
            assertEquals(-1, maxBatchTime.longValue());
            ServerManagement.setAttribute(on, "MaxBatchTime", "3000");
            maxBatchTime = (Long)ServerManagement.getAttribute(on, "MaxBatchTime");
            assertEquals(3000, maxBatchTime.longValue());
            ServerManagement.setAttribute(on, "MaxBatchTime", "-1");
         }         
         
         {
            String subName = (String)ServerManagement.getAttribute(on, "SubName");
            assertEquals(null, subName);
            ServerManagement.setAttribute(on, "SubName", "submarine");
            subName = (String)ServerManagement.getAttribute(on, "SubName");
            assertEquals("submarine", subName);
            ServerManagement.setAttribute(on, "SubName", null);
         }
         
         {
            String clientID = (String)ServerManagement.getAttribute(on, "ClientID");
            assertEquals(null, clientID);
            ServerManagement.setAttribute(on, "ClientID", "clientid-123");
            clientID = (String)ServerManagement.getAttribute(on, "ClientID");
            assertEquals("clientid-123", clientID);
            ServerManagement.setAttribute(on, "ClientID", null);
         }
         
         {
            Long failureRetryInterval = (Long)ServerManagement.getAttribute(on, "FailureRetryInterval");
            assertEquals(5000, failureRetryInterval.longValue());
            ServerManagement.setAttribute(on, "FailureRetryInterval", "10000");
            failureRetryInterval = (Long)ServerManagement.getAttribute(on, "FailureRetryInterval");
            assertEquals(10000, failureRetryInterval.longValue());
            ServerManagement.setAttribute(on, "FailureRetryInterval", "5000");
         } 
         
         {
            Integer maxRetries = (Integer)ServerManagement.getAttribute(on, "MaxRetries");
            assertEquals(-1, maxRetries.intValue());
            ServerManagement.setAttribute(on, "MaxRetries", "1000");
            maxRetries = (Integer)ServerManagement.getAttribute(on, "MaxRetries");
            assertEquals(1000, maxRetries.intValue());
            ServerManagement.setAttribute(on, "MaxRetries", "-1");
         }         
         
         ServerManagement.getServer(0).invoke(on, "start", new Object[0], new String[0]);
         
         //Should not be able to change attributes when bridge is started - need to stop first         
         
         {
            String cfLookup = (String)ServerManagement.getAttribute(on, "SourceConnectionFactoryLookup");
            assertEquals("/XAConnectionFactory", cfLookup);
            ServerManagement.setAttribute(on, "SourceConnectionFactoryLookup", "/Wibble");
            cfLookup = (String)ServerManagement.getAttribute(on, "SourceConnectionFactoryLookup");
            assertEquals("/XAConnectionFactory", cfLookup);            
         }
         
         {
            String cfLookup = (String)ServerManagement.getAttribute(on, "TargetConnectionFactoryLookup");
            assertEquals("/XAConnectionFactory", cfLookup);
            ServerManagement.setAttribute(on, "TargetConnectionFactoryLookup", "/Wibble");
            cfLookup = (String)ServerManagement.getAttribute(on, "TargetConnectionFactoryLookup");
            assertEquals("/XAConnectionFactory", cfLookup);
         }
         
         {
            String destLookup = (String)ServerManagement.getAttribute(on, "SourceDestinationLookup");
            assertEquals("/queue/sourceQueue", destLookup);
            ServerManagement.setAttribute(on, "SourceDestinationLookup", "/queue/WibbleQueue");
            destLookup = (String)ServerManagement.getAttribute(on, "SourceDestinationLookup");
            assertEquals("/queue/sourceQueue", destLookup);
         }
         
         {
            String destLookup = (String)ServerManagement.getAttribute(on, "TargetDestinationLookup");
            assertEquals("/queue/targetQueue", destLookup);
            ServerManagement.setAttribute(on, "TargetDestinationLookup", "/queue/WibbleQueue");
            destLookup = (String)ServerManagement.getAttribute(on, "TargetDestinationLookup");
            assertEquals("/queue/targetQueue", destLookup);
         }
         
         {
            String username = (String)ServerManagement.getAttribute(on, "SourceUsername");
            assertEquals(null, username);
            ServerManagement.setAttribute(on, "SourceUsername", "bob");
            username = (String)ServerManagement.getAttribute(on, "SourceUsername");
            assertEquals(null, username);
         }
         
         {
            String password = (String)ServerManagement.getAttribute(on, "SourcePassword");
            assertEquals(null, password);
            ServerManagement.setAttribute(on, "SourcePassword", "eek");
            password = (String)ServerManagement.getAttribute(on, "SourcePassword");
            assertEquals(null, password);
         }
         
         {
            String username = (String)ServerManagement.getAttribute(on, "TargetUsername");
            assertEquals(null, username);
            ServerManagement.setAttribute(on, "TargetUsername", "bob");
            username = (String)ServerManagement.getAttribute(on, "TargetUsername");
            assertEquals(null, username);
         }
         
         {
            String password = (String)ServerManagement.getAttribute(on, "TargetPassword");
            assertEquals(null, password);
            ServerManagement.setAttribute(on, "TargetPassword", "eek");
            password = (String)ServerManagement.getAttribute(on, "TargetPassword");
            assertEquals(null, password);
         }
         
         {
            Integer qos = (Integer)ServerManagement.getAttribute(on, "QualityOfServiceMode");
            assertEquals(Bridge.QOS_ONCE_AND_ONLY_ONCE, qos.intValue());
            ServerManagement.setAttribute(on, "QualityOfServiceMode", String.valueOf(Bridge.QOS_AT_MOST_ONCE));
            qos = (Integer)ServerManagement.getAttribute(on, "QualityOfServiceMode");
            assertEquals(new Integer(Bridge.QOS_ONCE_AND_ONLY_ONCE), qos);
         }
         
         {
            String selector = (String)ServerManagement.getAttribute(on, "Selector");
            assertEquals(null, selector);
            ServerManagement.setAttribute(on, "Selector", "god='dead'");
            selector = (String)ServerManagement.getAttribute(on, "Selector");
            assertEquals(null, selector);
         }
         
         {
            Integer maxBatchSize = (Integer)ServerManagement.getAttribute(on, "MaxBatchSize");
            assertEquals(1, maxBatchSize.intValue());
            ServerManagement.setAttribute(on, "MaxBatchSize", "10");
            maxBatchSize = (Integer)ServerManagement.getAttribute(on, "MaxBatchSize");
            assertEquals(1, maxBatchSize.intValue());
         }
         
         {
            Long maxBatchTime = (Long)ServerManagement.getAttribute(on, "MaxBatchTime");
            assertEquals(-1, maxBatchTime.longValue());
            ServerManagement.setAttribute(on, "MaxBatchTime", "3000");
            maxBatchTime = (Long)ServerManagement.getAttribute(on, "MaxBatchTime");
            assertEquals(-1, maxBatchTime.longValue());
         }         
         
         {
            String subName = (String)ServerManagement.getAttribute(on, "SubName");
            assertEquals(null, subName);
            ServerManagement.setAttribute(on, "SubName", "submarine");
            subName = (String)ServerManagement.getAttribute(on, "SubName");
            assertEquals(null, subName);
         }
         
         {
            String clientID = (String)ServerManagement.getAttribute(on, "ClientID");
            assertEquals(null, clientID);
            ServerManagement.setAttribute(on, "ClientID", "clientid-123");
            clientID = (String)ServerManagement.getAttribute(on, "ClientID");
            assertEquals(null, clientID);
         }
         
         {
            Long failureRetryInterval = (Long)ServerManagement.getAttribute(on, "FailureRetryInterval");
            assertEquals(5000, failureRetryInterval.longValue());
            ServerManagement.setAttribute(on, "FailureRetryInterval", "10000");
            failureRetryInterval = (Long)ServerManagement.getAttribute(on, "FailureRetryInterval");
            assertEquals(5000, failureRetryInterval.longValue());
         } 
         
         {
            Integer maxRetries = (Integer)ServerManagement.getAttribute(on, "MaxRetries");
            assertEquals(-1, maxRetries.intValue());
            ServerManagement.setAttribute(on, "MaxRetries", "1000");
            maxRetries = (Integer)ServerManagement.getAttribute(on, "MaxRetries");
            assertEquals(-1, maxRetries.intValue());
         }         
         
         InitialContext icSource = new InitialContext(props1);
         InitialContext icTarget = new InitialContext(props2);
         
         log.trace("Checking bridged bridge");
         
         checkBridged(icSource, icTarget, "/ConnectionFactory", "/ConnectionFactory",
                      "/queue/sourceQueue", "/queue/targetQueue");
         
         log.trace("Checked bridge");
         
         
      }
      finally
      {
         ServerManagement.undeployQueue("sourceQueue", 1);
         ServerManagement.undeployQueue("targetQueue", 2);
      }
            
   }
   
   private String tableToString(Properties t) throws Exception
   {
      ByteArrayOutputStream boa = new ByteArrayOutputStream();      
      
      t.store(boa, "");
      
      return new String(boa.toByteArray());
   }
   
   private ObjectName deployBridge(int server, String bridgeName, String sourceCFLookup, String targetCFLookup,
            String sourceDestLookup, String targetDestLookup,
            String sourceUsername, String sourcePassword,
            String targetUsername, String targetPassword,
            int qos, String selector, int maxBatchSize,
            long maxBatchTime, String subName, String clientID,
            long failureRetryInterval, int maxRetries,
            String sourceJNDIProperties,
            String targetJNDIProperties) throws Exception
   {
      String config = 
         "<mbean code=\"org.jboss.jms.server.bridge.BridgeService\" " +
         "name=\"jboss.messaging:service=Bridge,name=" + bridgeName + "\" " +
         "xmbean-dd=\"xmdesc/Bridge-xmbean.xml\">" +      
         "<attribute name=\"SourceConnectionFactoryLookup\">" + sourceCFLookup + "</attribute>"+      
         "<attribute name=\"TargetConnectionFactoryLookup\">" + targetCFLookup + "</attribute>"+     
         "<attribute name=\"SourceDestinationLookup\">" + sourceDestLookup + "</attribute>"+     
         "<attribute name=\"TargetDestinationLookup\">" + targetDestLookup + "</attribute>";
      if (sourceUsername != null)
      {
         config += "<attribute name=\"SourceUsername\">" + sourceUsername + "</attribute>";
      }
      if (sourcePassword != null)
      {
         config += "<attribute name=\"SourcePassword\">" + sourcePassword +"</attribute>";
      }
      if (targetUsername != null)
      {
         config +=  "<attribute name=\"TargetUsername\">" + targetUsername +"</attribute>";
      }
      if (targetPassword != null)
      {
         config += "<attribute name=\"TargetPassword\">" + targetPassword + "</attribute>";
      }
      config += "<attribute name=\"QualityOfServiceMode\">" + qos +"</attribute>";
      if (selector != null)
      {
         config += "<attribute name=\"Selector\">" + selector + "</attribute>";
      }
      config += "<attribute name=\"MaxBatchSize\">" + maxBatchSize + "</attribute>"+           
      "<attribute name=\"MaxBatchTime\">" + maxBatchTime +"</attribute>";
      if (subName != null)
      {
         config += "<attribute name=\"SubName\">" + subName + "</attribute>";
      }
      if (clientID != null)
      {
         config += "<attribute name=\"ClientID\">" + clientID + "</attribute>";
      }
      config += "<attribute name=\"FailureRetryInterval\">" + failureRetryInterval + "</attribute>"+      
      "<attribute name=\"MaxRetries\">" + maxRetries +"</attribute>";
      if (sourceJNDIProperties != null)
      {
         config += "<attribute name=\"SourceJNDIProperties\"><![CDATA["+
         sourceJNDIProperties +
         "]]>"+
         "</attribute>";
      }
      if (targetJNDIProperties != null)
      {
         config += "<attribute name=\"TargetJNDIProperties\"><![CDATA["+
         targetJNDIProperties +
         "]]>"+
         "</attribute>";
      }
      config += "</mbean>";
      
      return ServerManagement.getServer(server).deploy(config);            
   }
   
   private void checkBridged(InitialContext icSource, InitialContext icTarget,
            String sourceCFLookup, String targetCFLookup,
            String sourceDestLookup, String targetDestLookup)
      throws Exception
   {
      Connection connSource = null;
      
      Connection connTarget = null;
      
      try
      {
         ConnectionFactory cf0 = (ConnectionFactory)icSource.lookup(sourceCFLookup);
         
         ConnectionFactory cf1 = (ConnectionFactory)icTarget.lookup(targetCFLookup);
         
         Destination destSource = (Destination)icSource.lookup(sourceDestLookup);
         
         Destination destTarget = (Destination)icTarget.lookup(targetDestLookup);
         
         connSource = cf0.createConnection();
         
         connTarget = cf1.createConnection();
         
         connTarget.start();
         
         connSource.start();
         
         final int NUM_MESSAGES = 50;
         
         Session sessSource = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSource.createProducer(destSource);
         
         Session sessTarget = connTarget.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessTarget.createConsumer(destTarget);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSource.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(10000);
            
            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         Message m = cons.receive(1000);
         
         assertNull(m);
         
         MessageConsumer cons2 = sessSource.createConsumer(destSource);
         
         m = cons2.receive(1000);
         
         assertNull(m);
         
      }
      finally
      {
         if (connSource != null)
         {
            connSource.close();
         }
         
         if (connTarget != null)
         {
            connTarget.close();
         }
      }
   }
   
}
