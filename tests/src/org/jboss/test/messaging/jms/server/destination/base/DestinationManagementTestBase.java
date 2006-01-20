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

import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.xml.XMLUtil;
import org.w3c.dom.Element;

import java.util.Set;

/**
 * Exercises a destinatio's management interface after deployment.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class DestinationManagementTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DestinationManagementTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("this test is not supposed to run in a remote configuration!");
      }

      super.setUp();
      ServerManagement.start("all");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testDeployDestination() throws Exception
   {
      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@\" " +
         "       name=\"somedomain:service=@TOREPLACE@,name=Kirkwood\"" +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "</mbean>";

      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);

      assertEquals("Kirkwood", ServerManagement.getAttribute(destObjectName, "Name"));

      String jndiName = (isQueue() ? "/queue" : "/topic") + "/Kirkwood";
      assertEquals(jndiName, ServerManagement.getAttribute(destObjectName, "JNDIName"));


      Set destinations = (Set)ServerManagement.invoke(serverPeerObjectName, "getDestinations",
                                                      new Object[0], new String[0]);

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

   public void testDefaultSecurityConfiguration() throws Exception
   {
      String config =
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@\" " +
         "       name=\"somedomain:service=@TOREPLACE@,name=DefaultSecurity\"" +
         "       xmbean-dd=\"xmdesc/@TOREPLACE@-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "</mbean>";

      config = adjustConfiguration(config);

      ObjectName destObjectName = deploy(config);

      Element defaultSecurity =
         (Element)ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(),
                                                "DefaultSecurityConfig");

      Element security = (Element)ServerManagement.getAttribute(destObjectName, "SecurityConfig");

      XMLUtil.assertEquivalent(defaultSecurity, security);

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
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@\"\n" +
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
         "<mbean code=\"org.jboss.jms.server.destination.@TOREPLACE@\" " +
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

      ServerManagement.undeploy(destObjectName);
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract boolean isQueue();

   // Private -------------------------------------------------------

   private ObjectName deploy(String destConfig) throws Exception
   {
      ObjectName on = ServerManagement.deploy(destConfig);

      ServerManagement.invoke(on, "create", new Object[0], new String[0]);
      ServerManagement.invoke(on, "start", new Object[0], new String[0]);

      return on;
   }

   private String adjustConfiguration(String config)
   {
      return config.replaceAll("@TOREPLACE@", isQueue() ? "Queue" : "Topic");
   }

   private void undeployDestination(String name) throws Exception
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

   // Inner classes -------------------------------------------------
}
