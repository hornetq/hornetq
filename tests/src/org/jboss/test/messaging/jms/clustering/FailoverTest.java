/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.FailoverListener;
import org.jboss.jms.client.FailoverEvent;
import org.jboss.jms.client.Valve;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.JBossQueueBrowser;
import org.jboss.jms.client.JBossMessageConsumer;
import org.jboss.jms.client.JBossMessageProducer;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.delegate.DelegateSupport;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.QueueBrowser;
import javax.jms.TextMessage;
import javax.jms.DeliveryMode;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class FailoverTest extends ClusteringTestBase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public FailoverTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testSimpleConnectionFailover() throws Exception
   {
      Connection conn = null;

      try
      {
         // skip connection to node 0
         conn = cf.createConnection();
         conn.close();

         // create a connection to node 1
         conn = cf.createConnection();
         conn.start();

         assertEquals(1, ((JBossConnection)conn).getServerID());

         // register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn).registerFailoverListener(failoverListener);

         log.debug("killing node 1 ....");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(120000);
            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }

         // failover complete
         log.info("failover completed");

         assertEquals(0, ((JBossConnection)conn).getServerID());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testConnectionAndSessionFailover() throws Exception
   {
      Connection conn = null;

      try
      {
         // skip connection to node 0
         conn = cf.createConnection();
         conn.close();

         // create a connection to node 1
         conn = cf.createConnection();
         conn.start();

         assertEquals(1, ((JBossConnection)conn).getServerID());

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // register a failover listener
         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn).registerFailoverListener(failoverListener);

         log.debug("killing node 1 ....");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait for the client-side failover to complete

         while(true)
         {
            FailoverEvent event = failoverListener.getEvent(120000);
            if (event != null && FailoverEvent.FAILOVER_COMPLETED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }

         // failover complete
         log.info("failover completed");

         assertEquals(0, ((JBossConnection)conn).getServerID());

         // use the old session to send/receive a message
         session.createProducer(queue[0]).send(session.createTextMessage("blik"));

         TextMessage m = (TextMessage)session.createConsumer(queue[0]).receive(2000);

         assertEquals("blik", m.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }


   public void testFailoverListener() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         conn.close();

         conn = cf.createConnection();
         conn.start();

         // create a producer/consumer on node 1 and make sure we're connecting to node 1

         int nodeID = ((ConnectionState)((DelegateSupport)((JBossConnection)conn).
            getDelegate()).getState()).getServerID();

         assertEquals(1, nodeID);

         SimpleFailoverListener failoverListener = new SimpleFailoverListener();
         ((JBossConnection)conn).registerFailoverListener(failoverListener);

         // kill node 1

         log.debug("killing node 1");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         FailoverEvent event = failoverListener.getEvent(120000);

         assertNotNull(event);
         assertEquals(FailoverEvent.FAILURE_DETECTED, event.getType());
         log.info("got " + event);

         event = failoverListener.getEvent(120000);

         assertNotNull(event);
         assertEquals(FailoverEvent.FAILOVER_STARTED, event.getType());
         log.info("got " + event);

         event = failoverListener.getEvent(120000);

         assertNotNull(event);
         assertEquals(FailoverEvent.FAILOVER_COMPLETED, event.getType());
         log.info("got " + event);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testCloseValveHierarchy() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         assertTrue(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());

         Session session1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         assertTrue(((Valve)((JBossSession)session1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossSession)session2).getDelegate()).isValveOpen());

         MessageProducer prod1 = session1.createProducer(queue[0]);
         assertTrue(((Valve)((JBossMessageProducer)prod1).getDelegate()).isValveOpen());

         MessageConsumer cons1 = session1.createConsumer(queue[0]);
         assertTrue(((Valve)((JBossMessageConsumer)cons1).getDelegate()).isValveOpen());

         QueueBrowser browser1 = session1.createBrowser(queue[0]);
         assertTrue(((Valve)((JBossQueueBrowser)browser1).getDelegate()).isValveOpen());

         MessageProducer prod2 = session2.createProducer(queue[0]);
         assertTrue(((Valve)((JBossMessageProducer)prod2).getDelegate()).isValveOpen());

         MessageConsumer cons2 = session2.createConsumer(queue[0]);
         assertTrue(((Valve)((JBossMessageConsumer)cons2).getDelegate()).isValveOpen());

         QueueBrowser browser2 = session2.createBrowser(queue[0]);
         assertTrue(((Valve)((JBossQueueBrowser)browser2).getDelegate()).isValveOpen());

         ((JBossConnection)conn).getDelegate().closeValve();

         log.debug("top level valve closed");

         assertFalse(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossSession)session1).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossSession)session2).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageProducer)prod1).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageConsumer)cons1).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossQueueBrowser)browser1).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageProducer)prod2).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossMessageConsumer)cons2).getDelegate()).isValveOpen());
         assertFalse(((Valve)((JBossQueueBrowser)browser2).getDelegate()).isValveOpen());

         ((JBossConnection)conn).getDelegate().openValve();

         log.debug("top level valve open");

         assertTrue(((Valve)((JBossConnection)conn).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossSession)session1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossSession)session2).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageProducer)prod1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageConsumer)cons1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossQueueBrowser)browser1).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageProducer)prod2).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossMessageConsumer)cons2).getDelegate()).isValveOpen());
         assertTrue(((Valve)((JBossQueueBrowser)browser2).getDelegate()).isValveOpen());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testFailoverMessageOnServer() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         conn.close();

         conn = cf.createConnection();
         conn.start();

         assertEquals(1, ((JBossConnection)conn).getServerID());

         SimpleFailoverListener listener = new SimpleFailoverListener();
         ((JBossConnection)conn).registerFailoverListener(listener);

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue[1]);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         MessageConsumer cons = session.createConsumer(queue[0]);


         // send a message

         prod.send(session.createTextMessage("blip"));

         // kill node 1

         log.debug("killing node 1");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait until the failure (not the completion of client-side failover) is detected

         while(true)
         {
            FailoverEvent event = listener.getEvent(120000);
            if (event != null && FailoverEvent.FAILOVER_STARTED == event.getType())
            {
               break;
            }
            if (event == null)
            {
               fail("Did not get expected FAILOVER_COMPLETED event");
            }
         }

         // start to receive the very next moment the failure is detected. This way, we also
         // test the client-side failover valve

         TextMessage tm = (TextMessage)cons.receive(60000);
         assertEquals("blip", tm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // TODO http://jira.jboss.org/jira/browse/JBMESSAGING-712
//   public void testFailoverMessageOnServer2() throws Exception
//   {
//      Connection conn = null;
//
//      try
//      {
//         conn = cf.createConnection();
//         conn.close();
//
//         conn = cf.createConnection();
//         conn.start();
//
//         assertEquals(1, ((JBossConnection)conn).getServerID());
//
//         SimpleFailoverListener listener = new SimpleFailoverListener();
//         ((JBossConnection)conn).registerFailoverListener(listener);
//
//         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageProducer prod = session.createProducer(queue[1]);
//         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
//
//         // send a message
//
//         prod.send(session.createTextMessage("blip"));
//
//         // kill node 1
//
//         log.debug("killing node 1");
//
//         ServerManagement.kill(1);
//
//         log.info("########");
//         log.info("######## KILLED NODE 1");
//         log.info("########");
//
//         // wait until the failure (not the completion of client-side failover) is detected
//
//         assertEquals(FailoverEvent.FAILURE_DETECTED, listener.getEvent(60000).getType());
//
//         // create a consumer the very next moment the failure is detected. This way, we also
//         // test the client-side failover valve
//
//         MessageConsumer cons = session.createConsumer(queue[0]);
//
//         // we must receive the message
//
//         TextMessage tm = (TextMessage)cons.receive(60000);
//         assertEquals("blip", tm.getText());
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 2;

      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private class SimpleFailoverListener implements FailoverListener
   {
      private LinkedQueue buffer;

      public SimpleFailoverListener()
      {
         buffer = new LinkedQueue();
      }

      public void failoverEventOccured(FailoverEvent event)
      {
         try
         {
            buffer.put(event);
         }
         catch(InterruptedException e)
         {
            throw new RuntimeException("Putting thread interrupted while trying to add event " +
               "to buffer", e);
         }
      }

      /**
       * Blocks until a FailoverEvent is available or timeout occurs, in which case returns null.
       */
      public FailoverEvent getEvent(long timeout) throws InterruptedException
      {
         return (FailoverEvent)buffer.poll(timeout);
      }
   }

}
