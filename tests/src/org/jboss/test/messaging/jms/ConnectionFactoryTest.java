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

import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.naming.InitialContext;
import javax.management.ObjectName;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
         "       name=\"jboss.messaging.destination:service=TestConnectionFactory\"\n" +
         "       xmbean-dd=\"xmdesc/ConnectionFactory-xmbean.xml\">\n" +
         "       <constructor>\n" +
         "           <arg type=\"java.lang.String\" value=\"sofiavergara\"/>\n" +
         "       </constructor>\n" +
         "       <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>\n" +
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

      c.close();

      ServerManagement.invoke(on, "stop", new Object[0], new String[0]);
      ServerManagement.invoke(on, "destroy", new Object[0], new String[0]);
      ServerManagement.undeploy(on);

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

      c.setClientID("sofiavergara");
      assertEquals("sofiavergara", c.getClientID());

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
   
   // Inner classes -------------------------------------------------

}
