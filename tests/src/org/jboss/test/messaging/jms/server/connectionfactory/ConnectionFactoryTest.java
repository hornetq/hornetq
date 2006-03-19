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
package org.jboss.test.messaging.jms.server.connectionfactory;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.jms.ConnectionFactory;
import javax.jms.XAConnectionFactory;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnectionFactory;
import javax.management.ObjectName;

/**
 * Tests a deployed ConnectionFactory service.
 *
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

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("this test is not supposed to run in a remote configuration!");
      }

      super.setUp();
      ServerManagement.start("all");

      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();

      initialContext.close();

      ServerManagement.stop();
   }

   public void testDefaultConnectionFactory() throws Exception
   {
      // I expect at least "/ConnectionFactory" and "/XAConnectionFactory", they should be
      // configured by default in jboss-service.xml

      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      log.debug("ConnectionFactory: " + cf);

      XAConnectionFactory xacf = (XAConnectionFactory)initialContext.lookup("/XAConnectionFactory");
      log.debug("ConnectionFactory: " + xacf);

      cf = (ConnectionFactory)initialContext.lookup("java:/ConnectionFactory");
      log.debug("ConnectionFactory: " + cf);

      xacf = (XAConnectionFactory)initialContext.lookup("java:/XAConnectionFactory");
      log.debug("ConnectionFactory: " + xacf);
   }

   public void testDeployment() throws Exception
   {
      String objectName = "somedomain:service=SomeConnectionFactory";
      String[] jndiBindings = new String[] { "/SomeConnectionFactory" };

      ServerManagement.deployConnectionFactory(objectName, jndiBindings);

      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/SomeConnectionFactory");

      assertNotNull(cf);
      assertTrue(cf instanceof QueueConnectionFactory);
      assertTrue(cf instanceof TopicConnectionFactory);

      ServerManagement.undeployConnectionFactory(new ObjectName(objectName));

      try
      {
         initialContext.lookup("/SomeConnectionFactory");
         fail("should throw exception");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }

   public void testDeploymentMultipleJNDIBindings() throws Exception
   {
      String objectName = "somedomain:service=SomeConnectionFactory";
      String[] jndiBindings = new String[] { "/name1", "/name2", "/name3" };
      ServerManagement.deployConnectionFactory(objectName, jndiBindings);

      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/name1");
      assertNotNull(cf);

      cf = (ConnectionFactory)initialContext.lookup("/name2");
      assertNotNull(cf);

      cf = (ConnectionFactory)initialContext.lookup("/name2");
      assertNotNull(cf);

      ServerManagement.undeployConnectionFactory(new ObjectName(objectName));

      try
      {
         initialContext.lookup("/name1");
         fail("should throw exception");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      try
      {
         initialContext.lookup("/name2");
         fail("should throw exception");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      try
      {
         initialContext.lookup("/name3");
         fail("should throw exception");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }

   public void testDeploymentNewJNDIContext() throws Exception
   {
      String objectName = "somedomain:service=SomeConnectionFactory";
      String[] jndiBindings = new String[] { "/a/compound/jndi/name" };
      ServerManagement.deployConnectionFactory(objectName, jndiBindings);

      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/a/compound/jndi/name");
      assertNotNull(cf);

      ServerManagement.undeployConnectionFactory(new ObjectName(objectName));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
