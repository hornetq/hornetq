/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.bridge.QualityOfServiceMode;
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
   
   private static ObjectName sourceProviderLoader;
   
   private static ObjectName targetProviderLoader;
   
   public void setUp() throws Exception
   {
   	boolean first = firstTime;
   	
   	super.setUp();
   	
   	if (first)
   	{   	
	   	Properties props1 = new Properties();
	      props1.putAll(ServerManagement.getJNDIEnvironment(0));
	      
	      Properties props2 = new Properties();
	      props2.putAll(ServerManagement.getJNDIEnvironment(1));
	      
	      installJMSProviderLoader(0, props1, "/XAConnectionFactory", "adaptor1");
	      
	      installJMSProviderLoader(0, props2, "/XAConnectionFactory", "adaptor2");
	      
	      sourceProviderLoader = new ObjectName("jboss.messaging:service=JMSProviderLoader,name=adaptor1");
	      targetProviderLoader = new ObjectName("jboss.messaging:service=JMSProviderLoader,name=adaptor2");
   	}
   }
   
   public void tearDown() throws Exception
   {
   	super.tearDown();
   }
   
   public void testStopStartPauseResume() throws Exception
   {
      Connection connSource = null;
      
      Connection connTarget = null;
      
      ObjectName on = null;
      
   	try
   	{
	      on = deployBridge(0, "Bridge1", sourceProviderLoader, targetProviderLoader,
	                                   "/queue/sourceQueue", "/queue/targetQueue",
	                                   null, null, null, null,
	                                   QualityOfServiceMode.AT_MOST_ONCE, null, 1,
	                                   -1, null, null, 5000, -1, false);
	      log.info("Deployed bridge");
	      
	      ServerManagement.getServer(0).invoke(on, "create", new Object[0], new String[0]);
	      
	      log.info("Created bridge");
	      	          
         connSource = cf0.createConnection();
         
         connTarget = cf1.createConnection();
         
         connTarget.start();
         
         connSource.start();
         
         final int NUM_MESSAGES = 50;
         
         Session sessSource = connSource.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSource.createProducer(sourceQueue);
         
         Session sessTarget = connTarget.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons = sessTarget.createConsumer(targetQueue);
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sessSource.createTextMessage("message" + i);
            
            prod.send(tm);
         }
         
         //It's stopped so no messages should be received
         
         checkEmpty(targetQueue, 1);
         
         //Start it
         
         log.info("Starting bridge");
         ServerManagement.getServer(0).invoke(on, "start", new Object[0], new String[0]);
         log.info("Started bridge");
         
         //Now should receive the messages
                  
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(2000);
            
            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         checkEmpty(targetQueue, 1);
                  
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
         
         checkEmpty(targetQueue, 1);
         
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
         
         checkEmpty(targetQueue, 1);
         
         // Resume
         
         ServerManagement.getServer(0).invoke(on, "resume", new Object[0], new String[0]);
         
         //Now messages should be received
         
         for (int i = 2 * NUM_MESSAGES; i < 3 * NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(2000);
            
            assertNotNull(tm);

            assertEquals("message" + i, tm.getText());
         }
         
         checkEmpty(targetQueue, 1);
         
         isPaused = ((Boolean)ServerManagement.getAttribute(on, "Paused")).booleanValue();
         
         assertFalse(isPaused);
         
         //Stop
         
         ServerManagement.getServer(0).invoke(on, "stop", new Object[0], new String[0]);
         
         boolean isStarted = ((Boolean)ServerManagement.getAttribute(on, "Started")).booleanValue();
         
         assertFalse(isStarted); 
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
         
         try
         {
         	if (on != null)
         	{
         		ServerManagement.getServer(0).invoke(on, "stop", new Object[0], new String[0]);
         		ServerManagement.getServer(0).invoke(on, "destroy", new Object[0], new String[0]);
         	}
         }
         catch(Exception e)
         {
            //Ignore            
         }
      }
   }
         
   public void testDeploy() throws Exception
   {      
      ObjectName on = null;
      
      try
      {                
         on = deployBridge(0, "Bridge2", sourceProviderLoader, targetProviderLoader,
                           "/queue/sourceQueue", "/queue/targetQueue",
                           null, null, null, null,
                           QualityOfServiceMode.ONCE_AND_ONLY_ONCE, null, 1,
                           -1, null, null, 5000, -1, false);
         
         log.trace("Constructed bridge");
         
         ServerManagement.getServer(0).invoke(on, "create", new Object[0], new String[0]);
         
         log.trace("Created bridge");
            
         {
            ObjectName sourceProviderLoader2 = (ObjectName)ServerManagement.getAttribute(on, "SourceProviderLoader");
            assertEquals(sourceProviderLoader, sourceProviderLoader2);
            ServerManagement.setAttribute(on, "SourceProviderLoader", "jboss.messaging:service=JMSProviderLoader,name=blah");
            sourceProviderLoader2 = (ObjectName)ServerManagement.getAttribute(on, "SourceProviderLoader");
            assertEquals(new ObjectName("jboss.messaging:service=JMSProviderLoader,name=blah"), sourceProviderLoader2);
            ServerManagement.setAttribute(on, "SourceProviderLoader", sourceProviderLoader.toString());
         }
              
         {
         	ObjectName targetProviderLoader2 = (ObjectName)ServerManagement.getAttribute(on, "TargetProviderLoader");
            assertEquals(targetProviderLoader, targetProviderLoader2);
            ServerManagement.setAttribute(on, "TargetProviderLoader", "jboss.messaging:service=JMSProviderLoader,name=blah2");
            targetProviderLoader2 = (ObjectName)ServerManagement.getAttribute(on, "TargetProviderLoader");
            assertEquals(new ObjectName("jboss.messaging:service=JMSProviderLoader,name=blah2"), targetProviderLoader2);
            ServerManagement.setAttribute(on, "TargetProviderLoader", targetProviderLoader.toString());
  
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
            assertEquals(QualityOfServiceMode.ONCE_AND_ONLY_ONCE.intValue(), qos.intValue());
            ServerManagement.setAttribute(on, "QualityOfServiceMode", String.valueOf(QualityOfServiceMode.AT_MOST_ONCE.intValue()));
            qos = (Integer)ServerManagement.getAttribute(on, "QualityOfServiceMode");
            assertEquals(new Integer(QualityOfServiceMode.AT_MOST_ONCE.intValue()), qos);
            ServerManagement.setAttribute(on, "QualityOfServiceMode", String.valueOf(QualityOfServiceMode.ONCE_AND_ONLY_ONCE.intValue()));
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
            ObjectName sourceProviderLoader2 = (ObjectName)ServerManagement.getAttribute(on, "SourceProviderLoader");
            assertEquals(sourceProviderLoader, sourceProviderLoader2);
            ServerManagement.setAttribute(on, "SourceProviderLoader", "jboss.messaging:service=JMSProviderLoader,name=blah");
            sourceProviderLoader2 = (ObjectName)ServerManagement.getAttribute(on, "SourceProviderLoader");
            assertEquals(sourceProviderLoader, sourceProviderLoader2);
         }
              
         {
         	ObjectName targetProviderLoader2 = (ObjectName)ServerManagement.getAttribute(on, "TargetProviderLoader");
            assertEquals(targetProviderLoader, targetProviderLoader2);
            ServerManagement.setAttribute(on, "TargetProviderLoader", "jboss.messaging:service=JMSProviderLoader,name=blah2");
            targetProviderLoader2 = (ObjectName)ServerManagement.getAttribute(on, "TargetProviderLoader");
            assertEquals(targetProviderLoader, targetProviderLoader2);
 
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
            assertEquals(QualityOfServiceMode.ONCE_AND_ONLY_ONCE.intValue(), qos.intValue());
            ServerManagement.setAttribute(on, "QualityOfServiceMode", String.valueOf(QualityOfServiceMode.AT_MOST_ONCE.intValue()));
            qos = (Integer)ServerManagement.getAttribute(on, "QualityOfServiceMode");
            assertEquals(new Integer(QualityOfServiceMode.ONCE_AND_ONLY_ONCE.intValue()), qos);
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
         
         Properties props1 = new Properties();
	      props1.putAll(ServerManagement.getJNDIEnvironment(0));	      
	      Properties props2 = new Properties();
	      props2.putAll(ServerManagement.getJNDIEnvironment(1));
         InitialContext icSource = getInitialContext(0);
         InitialContext icTarget = getInitialContext(1);
         
         log.trace("Checking bridged bridge");
         
         checkBridged(icSource, icTarget, "/ConnectionFactory", "/ConnectionFactory",
                      "/queue/sourceQueue", "/queue/targetQueue");
         
         log.trace("Checked bridge");
         
      }
      finally
      {
         try
         {
            if (on != null)
            {
               ServerManagement.getServer(0).invoke(on, "stop", new Object[0], new String[0]);
               ServerManagement.getServer(0).invoke(on, "destroy", new Object[0], new String[0]);
            }
         }
         catch(Exception e)
         {
            //Ignore            
         }         
      }
            
   }
   
   
   private ObjectName deployBridge(int server, String bridgeName,
            ObjectName sourceProviderLoader, ObjectName targetProviderLoader,
            String sourceDestLookup, String targetDestLookup,
            String sourceUsername, String sourcePassword,
            String targetUsername, String targetPassword,
            QualityOfServiceMode qos, String selector, int maxBatchSize,
            long maxBatchTime, String subName, String clientID,
            long failureRetryInterval, int maxRetries, boolean addMessageIDInHeader) throws Exception
   {
      String config = 
         "<mbean code=\"org.jboss.messaging.jms.server.bridge.BridgeService\" " +
         "name=\"jboss.messaging:service=Bridge,name=" + bridgeName + "\" " +
         "xmbean-dd=\"xmdesc/Bridge-xmbean.xml\">" +      
         "<attribute name=\"SourceProviderLoader\">" + sourceProviderLoader + "</attribute>"+      
         "<attribute name=\"TargetProviderLoader\">" + targetProviderLoader + "</attribute>"+     
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
      config += "<attribute name=\"QualityOfServiceMode\">" + qos.intValue() +"</attribute>";
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
      config += "<attribute name=\"FailureRetryInterval\">" + failureRetryInterval + "</attribute>";    
      
      config += "<attribute name=\"MaxRetries\">" + maxRetries +"</attribute>";
      
      config += "<attribute name=\"AddMessageIDInHeader\">" + addMessageIDInHeader + "</attribute>";
      config += "</mbean>";
      
      // TODO: this has to be fixed
      // return ServerManagement.getServer(server).deploy(config);
      return null;
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
   
   private void installJMSProviderLoader(int server, Properties props, String factoryRef, String name)
      throws Exception
   {
   	ByteArrayOutputStream boa = new ByteArrayOutputStream();
   	props.store(boa, "");
   	String propsString =  new String(boa.toByteArray());

   	String config =
   		"<mbean code=\"org.jboss.jms.jndi.JMSProviderLoader\"" + 
   		" name=\"jboss.messaging:service=JMSProviderLoader,name=" + name + "\">" +
   		"<attribute name=\"ProviderName\">" + name + "</attribute>" +
   		"<attribute name=\"ProviderAdapterClass\">org.jboss.jms.jndi.JNDIProviderAdapter</attribute>" +
   		"<attribute name=\"FactoryRef\">" + factoryRef + "</attribute>" +
   		"<attribute name=\"QueueFactoryRef\">" + factoryRef + "</attribute>" +
   		"<attribute name=\"TopicFactoryRef\">" + factoryRef + "</attribute>" +
   		"<attribute name=\"Properties\">" + propsString + "</attribute></mbean>";
   	
   	log.info("Installing bridge: " + config);

      // TODO: this has to be fixed
      // ServerManagement.getServer(0).deploy(config);
   }

}
