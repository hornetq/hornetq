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

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * Connection tests. Contains all connection tests, except tests relating to closing a connection,
 * which go to ConnectionClosedTest.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
   
   
   private static final Logger log = Logger.getLogger(ConnectionTest.class);


   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected ConnectionFactory cf;
   protected Destination topic;

   // Constructors --------------------------------------------------

   public ConnectionTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.init("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      topic = (Destination)initialContext.lookup("/topic/Topic");

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.deInit();
      super.tearDown();
   }


   // Public --------------------------------------------------------
   
   //
   // Note: All tests related to closing a Connection should go to ConnectionClosedTest
   //

   public void testGetClientID() throws Exception
   {
      Connection connection = cf.createConnection();
      String clientID = connection.getClientID();

      //We don't currently set client ids on the server, so this should be null.
      //In the future we may provide conection factories that set a specific client id
      //so this may change
      assertNull(clientID);

      connection.close();
   }

   public void testSetClientID() throws Exception
   {
      Connection connection = cf.createConnection();

      final String clientID = "my-test-client-id";

      connection.setClientID(clientID);

      assertEquals(clientID, connection.getClientID());

      connection.close();
   }

   public void testSetClientIDFail() throws Exception
   {
      final String clientID = "my-test-client-id";

      //Setting a client id must be the first thing done to the connection
      //otherwise a javax.jms.IllegalStateException must be thrown

      Connection connection = cf.createConnection();
      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         connection.setClientID(clientID);
         fail();
      }
      catch (javax.jms.IllegalStateException e)
      {
         log.trace("Caught exception ok");
      }
      
      connection.close();


      connection = cf.createConnection();
      connection.getClientID();
      try
      {
         connection.setClientID(clientID);
         fail();
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      connection.close();

      connection = cf.createConnection();
      ExceptionListener listener = connection.getExceptionListener();
      try
      {
         connection.setClientID(clientID);
         fail();
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      connection.close();

      connection = cf.createConnection();
      connection.setExceptionListener(listener);
      try
      {
         connection.setClientID(clientID);
         fail();
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      connection.close();
   }

   public void testGetMetadata() throws Exception
   {
      Connection connection = cf.createConnection();

      ConnectionMetaData metaData = (ConnectionMetaData)connection.getMetaData();

      //TODO - need to check whether these are same as current version
      metaData.getJMSMajorVersion();
      metaData.getJMSMinorVersion();
      metaData.getJMSProviderName();
      metaData.getJMSVersion();
      metaData.getJMSXPropertyNames();
      metaData.getProviderMajorVersion();
      metaData.getProviderMinorVersion();
      metaData.getProviderVersion();

   }

   public void testStartStop() throws Exception
   {
      
      if (ServerManagement.isRemote())
      {
         //This test doesn't make sense remotely
         return;
      }

      Connection connection = cf.createConnection();
      Serializable connectionID = ((JBossConnection)connection).getConnectionID();

      ServerConnectionDelegate d = ServerManagement.getServerPeer().
            getClientManager().getConnectionDelegate(connectionID);

      assertFalse(d.isStarted());

      connection.start();

      assertTrue(d.isStarted());

      connection.stop();

      assertFalse(d.isStarted());

      connection.close();
   }

   /* Test creation of QueueSession */
   public void testQueueConnection1() throws Exception
   {
      QueueConnectionFactory qcf = (QueueConnectionFactory)cf;

      QueueConnection qc = qcf.createQueueConnection();

      qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      qc.close();

   }

   /* Test creation of TopicSession */
   public void testQueueConnection2() throws Exception
   {
      TopicConnectionFactory tcf = (TopicConnectionFactory)cf;

      TopicConnection tc = tcf.createTopicConnection();

      tc.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

      tc.close();

   }

   /* Test ExceptionListener stuff */
   public void testExceptionListener() throws Exception
   {
      Connection conn = cf.createConnection();

      ExceptionListener listener1 = new MyExceptionListener();

      conn.setExceptionListener(listener1);

      ExceptionListener listener2 = conn.getExceptionListener();

      assertNotNull(listener2);

      assertEquals(listener1, listener2);

      conn.close();

   }
   
   
//   Commented out for now, since how can i make the server fail from a test?   
//   public void testExceptionListenerFail() throws Exception
//   {
//      if (!ServerManagement.isRemote()) return;
//      
//      Connection conn = cf.createConnection();
//
//      MyExceptionListener listener1 = new MyExceptionListener();
//
//      conn.setExceptionListener(listener1);
//      
//      
//      RMIAdaptor rmiAdaptor = (RMIAdaptor) initialContext.lookup("jmx/invoker/RMIAdaptor");
//      
//      ObjectName on = new ObjectName("jboss.messaging:service=ServerPeer");
//      
//      rmiAdaptor.invoke(on, "stop",
//            new Object[] {}, new String[] {});
//      
//      try
//      {
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      }
//      catch (Exception ignore)
//      {}
//      
//
//      Thread.sleep(10000);
//      
//      if (listener1.exceptionReceived == null)
//      {
//         fail();
//      }
//      else
//      {
//         log.trace("Received exception:", listener1.exceptionReceived.getLinkedException());
//      }
//
//      try
//      {
//         conn.close();
//      }
//      catch (Exception e)
//      {}
//   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   static class MyExceptionListener implements ExceptionListener
   {
      JMSException exceptionReceived;
      
      public void onException(JMSException exception)
      {
         this.exceptionReceived = exception;
         log.trace("Received exception");
      }
   }
   
}
