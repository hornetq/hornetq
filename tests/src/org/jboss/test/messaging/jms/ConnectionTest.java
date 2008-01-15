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
import javax.jms.ConnectionMetaData;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.logging.Logger;


/**
 * Connection tests. Contains all connection tests, except tests relating to closing a connection,
 * which go to ConnectionClosedTest.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ConnectionTest.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   public void testResourceManagersForSameServer() throws Exception
   {
      Connection conn1 = cf.createConnection();      
            
      ClientConnectionDelegate del1 = getDelegate(conn1);
      
      ConnectionState state1 = (ConnectionState)del1.getState();
      
      ResourceManager rm1 = state1.getResourceManager();
      
      Connection conn2 = cf.createConnection();      
      
      ClientConnectionDelegate del2 = getDelegate(conn2);
      
      ConnectionState state2 = (ConnectionState)del2.getState();
      
      ResourceManager rm2 = state2.getResourceManager();

      //Two connections for same server should share the same resource manager
      
      assertTrue(rm1 == rm2);
      
      assertTrue(ResourceManagerFactory.instance.containsResourceManager(state2.getServerID()));
      
      conn1.close();
      
      //Check reference counting
      assertTrue(ResourceManagerFactory.instance.containsResourceManager(state2.getServerID()));
           
      conn2.close();
      
      assertFalse(ResourceManagerFactory.instance.containsResourceManager(state2.getServerID()));  
      
      assertEquals(0, ResourceManagerFactory.instance.size());
   }
   
   public void testResourceManagerFactory()
   {
      ResourceManagerFactory.instance.checkOutResourceManager(1);
      
      ResourceManagerFactory.instance.checkOutResourceManager(2);
      
      ResourceManagerFactory.instance.checkOutResourceManager(3);
      
      ResourceManagerFactory.instance.checkOutResourceManager(4);
      
      assertEquals(4, ResourceManagerFactory.instance.size());
      
      ResourceManagerFactory.instance.checkOutResourceManager(4);
      
      assertEquals(4, ResourceManagerFactory.instance.size());
      
      ResourceManagerFactory.instance.checkOutResourceManager(4);
      
      assertEquals(4, ResourceManagerFactory.instance.size());
      
      ResourceManagerFactory.instance.checkInResourceManager(4);
      
      assertEquals(4, ResourceManagerFactory.instance.size());
      
      ResourceManagerFactory.instance.checkInResourceManager(4);
      
      assertEquals(4, ResourceManagerFactory.instance.size());
      
      ResourceManagerFactory.instance.checkInResourceManager(4);
      
      assertEquals(3, ResourceManagerFactory.instance.size());
      
      ResourceManagerFactory.instance.checkInResourceManager(3);
      
      assertEquals(2, ResourceManagerFactory.instance.size());
      
      ResourceManagerFactory.instance.checkInResourceManager(2);
      
      assertEquals(1, ResourceManagerFactory.instance.size());
      
      ResourceManagerFactory.instance.checkInResourceManager(1);
      
      assertEquals(0, ResourceManagerFactory.instance.size());
 
   }
   
   public void testManyConnections() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         Connection conn = cf.createConnection();
         conn.close();        
      }
   }     
   
   

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

   public void testSetClientAfterStart() throws Exception
   {
   	Connection connection = null;
      try
      {
         connection = cf.createConnection();

         //we startthe connection
         connection.start();

         // an attempt to set the client ID now should throw a IllegalStateException
         connection.setClientID("testSetClientID_2");
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (JMSException e)
      {
         fail("Should raise a javax.jms.IllegalStateException, not a " + e);
      }
      catch (java.lang.IllegalStateException e)
      {
         fail("Should raise a javax.jms.IllegalStateException, not a java.lang.IllegalStateException");
      }
      finally
      {
      	if (connection != null)
      	{
      		connection.close();
      	}
      }
      
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
      
      connection.close();
   }


   /**
    * Test creation of QueueSession
    */
   public void testQueueConnection1() throws Exception
   {
      QueueConnectionFactory qcf = (QueueConnectionFactory)cf;

      QueueConnection qc = qcf.createQueueConnection();

      qc.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

      qc.close();
   }

   /**
    * Test creation of TopicSession
    */
   public void testQueueConnection2() throws Exception
   {
      TopicConnectionFactory tcf = (TopicConnectionFactory)cf;

      TopicConnection tc = tcf.createTopicConnection();

      tc.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

      tc.close();

   }

   /**
    * Test ExceptionListener stuff
    */
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
   
   /*
    * See http://jira.jboss.com/jira/browse/JBMESSAGING-635
    * 
    * This needs to be run remotely to see the exception
    */
   public void testConnectionListenerBug() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         Connection conn = cf.createConnection();
         
         MyExceptionListener listener = new MyExceptionListener();
         
         conn.setExceptionListener(listener);
         
         conn.close();        
         
         //The problem with this test is I would need to capture the output and search
         //for NullPointerException!!!                  
      } 
   }

   /**
    * This test is similar to a JORAM Test...
    * (UnifiedTest::testCreateDurableConnectionConsumerOnQueueConnection)
    *
    * @throws Exception
    */
   public void testDurableSubscriberOnQueueConnection() throws Exception
   {
      QueueConnection queueConnection = ((QueueConnectionFactory)cf).createQueueConnection();

      try
      {
         queueConnection.createDurableConnectionConsumer((Topic)topic1, "subscriptionName", "",
            (ServerSessionPool) null, 1);
         fail("Should throw a javax.jms.IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }
      catch (java.lang.IllegalStateException e)
      {
         fail ("Should throw a javax.jms.IllegalStateException");
      }
      catch (JMSException e)
      {
         fail("Should throw a javax.jms.IllegalStateException, not a " + e);
      }
      finally
      {
         queueConnection.close();
      }
   }     

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
