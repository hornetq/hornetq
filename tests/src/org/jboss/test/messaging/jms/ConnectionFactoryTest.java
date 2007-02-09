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
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public ConnectionFactoryTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   /**
    * Test that ConnectionFactory can be cast to QueueConnectionFactory and QueueConnection can be
    * created.
    */
   public void testQueueConnectionFactory() throws Exception
   {
      QueueConnectionFactory qcf =
         (QueueConnectionFactory)initialContext.lookup("/ConnectionFactory");
      QueueConnection qc = qcf.createQueueConnection();
      qc.close();
   }
   
   /**
    * Test that ConnectionFactory can be cast to TopicConnectionFactory and TopicConnection can be
    * created.
    */
   public void testTopicConnectionFactory() throws Exception
   {
      TopicConnectionFactory qcf =
         (TopicConnectionFactory)initialContext.lookup("/ConnectionFactory");
      TopicConnection tc = qcf.createTopicConnection();
      tc.close();
   }

   public void testAdministrativelyConfiguredClientID() throws Exception
   {
      // deploy a connection factory that has an administatively configured clientID

      String mbeanConfig =
         "<mbean code=\"org.jboss.jms.server.connectionfactory.ConnectionFactory\"\n" +
         "       name=\"jboss.messaging.connectionfactory:service=TestConnectionFactory\"\n" +
         "       xmbean-dd=\"xmdesc/ConnectionFactory-xmbean.xml\">\n" +
         "       <constructor>\n" +
         "           <arg type=\"java.lang.String\" value=\"sofiavergara\"/>\n" +
         "       </constructor>\n" +
         "       <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>\n" +
         "       <depends optional-attribute-name=\"Connector\">jboss.messaging:service=Connector,transport=socket</depends>\n" +
         "       <attribute name=\"JNDIBindings\">\n" +
         "          <bindings>\n" +
         "            <binding>/TestConnectionFactory</binding>\n" +
         "          </bindings>\n" +
         "       </attribute>\n" +
         " </mbean>";

      ObjectName on = ServerManagement.deploy(mbeanConfig);
      ServerManagement.invoke(on, "create", new Object[0], new String[0]);
      ServerManagement.invoke(on, "start", new Object[0], new String[0]);

      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/TestConnectionFactory");
      Connection c = cf.createConnection();

      assertEquals("sofiavergara", c.getClientID());

      try
      {
         c.setClientID("somethingelse");
         fail("should throw exception");

      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
      
      //Now try and deploy another one with the same client id
      
      mbeanConfig =
         "<mbean code=\"org.jboss.jms.server.connectionfactory.ConnectionFactory\"\n" +
         "       name=\"jboss.messaging.connectionfactory:service=TestConnectionFactory2\"\n" +
         "       xmbean-dd=\"xmdesc/ConnectionFactory-xmbean.xml\">\n" +
         "       <constructor>\n" +
         "           <arg type=\"java.lang.String\" value=\"sofiavergara\"/>\n" +
         "       </constructor>\n" +
         "       <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>\n" +
         "       <depends optional-attribute-name=\"Connector\">jboss.messaging:service=Connector,transport=socket</depends>\n" +
         "       <attribute name=\"JNDIBindings\">\n" +
         "          <bindings>\n" +
         "            <binding>/TestConnectionFactory2</binding>\n" +
         "          </bindings>\n" +
         "       </attribute>\n" +
         " </mbean>";
      
      ObjectName on2 = ServerManagement.deploy(mbeanConfig);
      ServerManagement.invoke(on2, "create", new Object[0], new String[0]);      
      ServerManagement.invoke(on2, "start", new Object[0], new String[0]);
      
      
      ServerManagement.invoke(on2, "stop", new Object[0], new String[0]);
      ServerManagement.invoke(on2, "destroy", new Object[0], new String[0]);
      ServerManagement.undeploy(on2);
      
      cf = (ConnectionFactory)initialContext.lookup("/TestConnectionFactory");
      Connection c2 = null;
      try
      {
         c2 = cf.createConnection();
      }
      catch (JMSException e)
      {
         //Ok
      }
      
      if (c2 != null) c2.close();
      
      c.close();

      ServerManagement.invoke(on, "stop", new Object[0], new String[0]);
      ServerManagement.invoke(on, "destroy", new Object[0], new String[0]);
      ServerManagement.undeploy(on);

   }
         
   public void testAdministrativelyConfiguredConnectors() throws Exception
   {
      //Deploy a few connectors
      String name1 = "jboss.messaging:service=Connector1,transport=socket";
      
      String name2 = "jboss.messaging:service=Connector2,transport=socket";
      
      String name3 = "jboss.messaging:service=Connector3,transport=socket";
      
      ObjectName c1 = deployConnector(1234, name1);
      ObjectName c2 = deployConnector(1235, name2);
      ObjectName c3 = deployConnector(1236, name3);
      
      ObjectName cf1 = deployConnectionFactory("jboss.messaging.destination:service=TestConnectionFactory1", name1, "/TestConnectionFactory1", "clientid1");
      ObjectName cf2 = deployConnectionFactory("jboss.messaging.destination:service=TestConnectionFactory2", name2, "/TestConnectionFactory2", "clientid2");
      ObjectName cf3 = deployConnectionFactory("jboss.messaging.destination:service=TestConnectionFactory3", name3, "/TestConnectionFactory3", "clientid3");
      //Last one shares the same connector
      ObjectName cf4 = deployConnectionFactory("jboss.messaging.destination:service=TestConnectionFactory4", name3, "/TestConnectionFactory4", "clientid4");
      
      
      JBossConnectionFactory f1 = (JBossConnectionFactory)initialContext.lookup("/TestConnectionFactory1");            
      ClientConnectionFactoryDelegate del1 = (ClientConnectionFactoryDelegate)f1.getDelegate();      
      
      assertTrue(del1.getServerLocatorURI().startsWith("socket://localhost:1234"));
      
      JBossConnectionFactory f2 = (JBossConnectionFactory)initialContext.lookup("/TestConnectionFactory2");            
      ClientConnectionFactoryDelegate del2 = (ClientConnectionFactoryDelegate)f2.getDelegate();      
      assertTrue(del2.getServerLocatorURI().startsWith("socket://localhost:1235"));
      
      JBossConnectionFactory f3 = (JBossConnectionFactory)initialContext.lookup("/TestConnectionFactory3");            
      ClientConnectionFactoryDelegate del3 = (ClientConnectionFactoryDelegate)f3.getDelegate();      
      assertTrue(del3.getServerLocatorURI().startsWith("socket://localhost:1236"));
      
      JBossConnectionFactory f4 = (JBossConnectionFactory)initialContext.lookup("/TestConnectionFactory4");            
      ClientConnectionFactoryDelegate del4 = (ClientConnectionFactoryDelegate)f4.getDelegate();      
      assertTrue(del4.getServerLocatorURI().startsWith("socket://localhost:1236"));
      
      Connection con1 = f1.createConnection();
      Connection con2 = f2.createConnection();
      Connection con3 = f3.createConnection();
      Connection con4 = f4.createConnection();
      con1.close();
      con2.close();
      con3.close();
      con4.close();
      
      stopService(cf1);
      stopService(cf2);
      stopService(cf3);
      
      //Check f4 is still ok
      Connection conn5 = f4.createConnection();
      conn5.close();
      
      stopService(cf4);
      
      stopService(c1);
      stopService(c2);
      stopService(c3);
   }
   
   public void testNoClientIDConfigured_1() throws Exception
   {
      // the ConnectionFactories that ship with Messaging do not have their clientID
      // administratively configured.

      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      Connection c = cf.createConnection();

      assertNull(c.getClientID());

      c.close();
   }

   public void testNoClientIDConfigured_2() throws Exception
   {
      // the ConnectionFactories that ship with Messaging do not have their clientID
      // administratively configured.

      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      Connection c = cf.createConnection();

      // set the client id immediately after the connection is created

      c.setClientID("sofiavergara2");
      assertEquals("sofiavergara2", c.getClientID());

      c.close();
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
      
      
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------
   
   private ObjectName deployConnector(int port, String name) throws Exception
   {
      String mbeanConfig =
         "<mbean code=\"org.jboss.remoting.transport.Connector\"\n" +
         " name=\"" +name + "\"\n" +
         " display-name=\"Socket transport Connector\">\n"  +        
        "<depends>jboss.messaging:service=NetworkRegistry</depends>\n" +
     "</mbean>";
      
      String config =
         "<attribute name=\"Configuration\">\n" +
         "<config>\n" +
            "<invoker transport=\"socket\">\n" +              
               "<attribute name=\"marshaller\" isParam=\"true\">org.jboss.jms.server.remoting.JMSWireFormat</attribute>\n" +
               "<attribute name=\"unmarshaller\" isParam=\"true\">org.jboss.jms.server.remoting.JMSWireFormat</attribute>\n" +
               "<attribute name=\"serializationtype\" isParam=\"true\">jms</attribute>\n" +
               "<attribute name=\"dataType\" isParam=\"true\">jms</attribute>\n" +
               "<attribute name=\"serverBindPort\">" + port +"</attribute>\n" +
               "<attribute name=\"socket.check_connection\" isParam=\"true\">false</attribute>\n" +
               "<attribute name=\"timeout\">0</attribute>\n" +
               "<attribute name=\"serverBindAddress\">localhost</attribute>\n" +
               "<attribute name=\"leasePeriod\">20000</attribute>\n" +  
               "<attribute name=\"clientSocketClass\" isParam=\"true\">org.jboss.jms.client.remoting.ClientSocketWrapper</attribute>\n" +
               "<attribute name=\"serverSocketClass\">org.jboss.jms.server.remoting.ServerSocketWrapper</attribute>\n" +
            "</invoker>\n" +
            "<handlers>\n" +
               "<handler subsystem=\"JMS\">org.jboss.jms.server.remoting.JMSServerInvocationHandler</handler>\n" +
            "</handlers>\n" +
         "</config>\n" +
      "</attribute>\n";
      
      ObjectName on = ServerManagement.deploy(mbeanConfig);
      
      ServerManagement.setAttribute(on, "Configuration", config);
            
      ServerManagement.invoke(on, "create", new Object[0], new String[0]);
      
      ServerManagement.invoke(on, "start", new Object[0], new String[0]);
      
      return on;
   }
   
   private ObjectName deployConnectionFactory(String name, String connectorName, String binding, String clientID) throws Exception
   {
      String mbeanConfig =
            "<mbean code=\"org.jboss.jms.server.connectionfactory.ConnectionFactory\"\n" +
            "       name=\"" + name + "\"\n" +
            "       xmbean-dd=\"xmdesc/ConnectionFactory-xmbean.xml\">\n" +
            "       <constructor>\n" +
            "           <arg type=\"java.lang.String\" value=\"" + clientID + "\"/>\n" +
            "       </constructor>\n" +
            "       <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>\n" +
            "       <depends optional-attribute-name=\"Connector\">" + connectorName + "</depends>\n" +
            "       <attribute name=\"JNDIBindings\">\n" +
            "          <bindings>\n" +
            "            <binding>" + binding + " </binding>\n" +
            "          </bindings>\n" +
            "       </attribute>\n" +
            " </mbean>";

      ObjectName on = ServerManagement.deploy(mbeanConfig);
      ServerManagement.invoke(on, "create", new Object[0], new String[0]);
      ServerManagement.invoke(on, "start", new Object[0], new String[0]);
      
      return on;
   }
   
   private void stopService(ObjectName on) throws Exception
   {
      ServerManagement.invoke(on, "stop", new Object[0], new String[0]);
      ServerManagement.invoke(on, "destroy", new Object[0], new String[0]);
      ServerManagement.undeploy(on);
   }
   
   // Inner classes -------------------------------------------------

}
