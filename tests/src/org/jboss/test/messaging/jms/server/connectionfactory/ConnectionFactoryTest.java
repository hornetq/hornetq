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

import org.jboss.jms.client.JBossMessageConsumer;
import org.jboss.jms.client.delegate.ClientConsumerDelegate;
import org.jboss.jms.client.state.ConsumerState;
import org.jboss.test.messaging.JBMServerTestCase;

import javax.jms.*;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

/**
 * Tests a deployed ConnectionFactory service.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------
   InitialContext ic;
   public ConnectionFactoryTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      ic = getInitialContext();
   }

   // Public --------------------------------------------------------
 
   public void testDefaultConnectionFactory() throws Exception
   {
      // These should be configured by default in connection-factories-service.xml

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      log.debug("ConnectionFactory: " + cf);
      
      XAConnectionFactory xacf = (XAConnectionFactory)ic.lookup("/XAConnectionFactory");
      log.debug("ConnectionFactory: " + xacf);

      cf = (ConnectionFactory)ic.lookup("java:/ConnectionFactory");
      log.debug("ConnectionFactory: " + cf);

      xacf = (XAConnectionFactory)ic.lookup("java:/XAConnectionFactory");
      log.debug("ConnectionFactory: " + xacf);
      
      cf = (ConnectionFactory)ic.lookup("/ClusteredConnectionFactory");
      log.debug("ConnectionFactory: " + cf);

      xacf = (XAConnectionFactory)ic.lookup("/ClusteredXAConnectionFactory");
      log.debug("ConnectionFactory: " + xacf);

      cf = (ConnectionFactory)ic.lookup("java:/ClusteredConnectionFactory");
      log.debug("ConnectionFactory: " + cf);

      xacf = (XAConnectionFactory)ic.lookup("java:/ClusteredXAConnectionFactory");
      log.debug("ConnectionFactory: " + xacf);
   }

   public void testDeployment() throws Exception
   {
      String objectName = "SomeConnectionFactory";
      String[] jndiBindings = new String[] { "/SomeConnectionFactory" };

      deployConnectionFactory(objectName, jndiBindings);

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/SomeConnectionFactory");

      assertNotNull(cf);
      assertTrue(cf instanceof QueueConnectionFactory);
      assertTrue(cf instanceof TopicConnectionFactory);

      undeployConnectionFactory(objectName);

      try
      {
         ic.lookup("/SomeConnectionFactory");
         fail("should throw exception");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }
   }
   
   public void testDeploymentWithPrefetch() throws Exception
   {
      String objectName = "SomeConnectionFactory";
      String[] jndiBindings = new String[] { "/SomeConnectionFactory" };

      final int prefetchSize = 777777;
      
      deployConnectionFactory(objectName, jndiBindings, prefetchSize);

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/SomeConnectionFactory");

      assertNotNull(cf);
      assertTrue(cf instanceof QueueConnectionFactory);
      assertTrue(cf instanceof TopicConnectionFactory);
      
      
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      JBossMessageConsumer cons = (JBossMessageConsumer)sess.createConsumer(queue1);
	      
	      ClientConsumerDelegate del = (ClientConsumerDelegate)cons.getDelegate();
	      
	      ConsumerState state = (ConsumerState)del.getState();
	      
	      int size = state.getBufferSize();
	      
	      assertEquals(prefetchSize, size);
	
	      undeployConnectionFactory(objectName);
	      
	      undeployQueue("testQueue");
	
	      try
	      {
	         ic.lookup("/SomeConnectionFactory");
	         fail("should throw exception");
	      }
	      catch(NameNotFoundException e)
	      {
	         // OK
	      }
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }

   public void testDeploymentMultipleJNDIBindings() throws Exception
   {
      String objectName = "somedomain:service=SomeConnectionFactory";
      String[] jndiBindings = new String[] { "/name1", "/name2", "/name3" };
      deployConnectionFactory(objectName, jndiBindings);

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/name1");
      assertNotNull(cf);

      cf = (ConnectionFactory)ic.lookup("/name2");
      assertNotNull(cf);

      cf = (ConnectionFactory)ic.lookup("/name2");
      assertNotNull(cf);

      undeployConnectionFactory(objectName);

      try
      {
         ic.lookup("/name1");
         fail("should throw exception");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      try
      {
         ic.lookup("/name2");
         fail("should throw exception");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      try
      {
         ic.lookup("/name3");
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
      deployConnectionFactory(objectName, jndiBindings);

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/a/compound/jndi/name");
      assertNotNull(cf);

      undeployConnectionFactory(objectName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
