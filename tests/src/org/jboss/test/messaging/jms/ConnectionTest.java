/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.ConnectionMetaData;
import javax.jms.MessageProducer;
import javax.jms.Message;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.MessageConsumer;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.jms.client.JBossConnection;

import java.io.Serializable;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

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
      ServerManagement.setRemote(false);
      ServerManagement.startInVMServer("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      topic = (Destination)initialContext.lookup("/topic/Topic");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      super.tearDown();
   }


   // Public --------------------------------------------------------

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

   public void testGetMetadataOnClosedConnection() throws Exception
   {
      Connection connection = cf.createConnection();
      connection.close();

      try
      {
         connection.getMetaData();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
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

   /* Tests for closing connection
    * ============================
    */


   /* Simple close */
   public void testClose1() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.close();
   }

   /* Close twice - second close should do nothing */
   public void testClose2() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.close();
      conn.close();
   }

   /*
    * A close terminates all pending message receives on the connection’s session’s
    * consumers. The receives may return with a message or null depending on
    * whether or not there was a message available at the time of the close.
    */
   public void testClose3() throws Exception
   {
      Connection conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      conn.start();

      final MessageConsumer consumer = session.createConsumer(topic);

      class TestRunnable implements Runnable
      {
         public String failed;

         public void run()
         {
            try
            {
               long start = System.currentTimeMillis();
               Message m = consumer.receive(2100);
               if (System.currentTimeMillis() - start >= 2000)
               {
                  //It timed out
                  failed = "Timed out";
               }
               else
               {
                  if (m != null)
                  {
                     failed = "Message Not null";
                  }
               }
            }
            catch(Exception e)
            {
               log.error(e);
               failed = e.getMessage();
            }
         }
      }

      TestRunnable runnable = new TestRunnable();
      Thread t = new Thread(runnable);
      t.start();

      Thread.sleep(1000);

      conn.close();

      t.join();

      if (runnable.failed != null)
      {
         fail(runnable.failed);
      }

   }

   /* Shouldn't be able to create a session after connection is closed
    */
   public void testClose4() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.close();

      try
      {
         conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         fail("Did not throw javax.jms.IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {

      }
   }

   /* Test that close() hierarchically closes all child objects */
   public void testClose5() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      sess.createConsumer(topic);
      MessageProducer producer = sess.createProducer(topic);

      Message m = sess.createMessage();


      conn.close();

      /* If the session is closed then any method invocation apart from close()
       * will throw an IllegalStateException
       */
      try
      {
         sess.createMessage();
         fail("Session is not closed");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }


      /* If the producer is closed then any method invocation apart from close()
       * will throw an IllegalStateException
       */

      try
      {
         producer.send(m);
         fail("Producer is not closed");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }

      //TODO Consumer and browser

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

      //TODO Simulate a problem with a connection and check the exception
      //is received on the listener

      ExceptionListener listener1 = new MyExceptionListener();

      conn.setExceptionListener(listener1);

      ExceptionListener listener2 = conn.getExceptionListener();

      assertNotNull(listener2);

      assertEquals(listener1, listener2);

      conn.close();

   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private static class MyExceptionListener implements ExceptionListener
   {
      public void onException(JMSException exception)
      {

      }
   }
   
}
