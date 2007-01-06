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
import javax.jms.Message;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;

import java.util.Enumeration;
import java.util.Set;
import java.util.HashSet;

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

   public void testSessionFailover() throws Exception
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

   public void testProducerFailover() throws Exception
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
         MessageProducer prod = session.createProducer(queue[1]);

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

         // send a message, send it with the failed over producer and make sure I can receive it
         Message m = session.createTextMessage("clik");
         prod.send(m);

         MessageConsumer cons = session.createConsumer(queue[0]);
         TextMessage tm = (TextMessage)cons.receive(2000);

         assertNotNull(tm);
         assertEquals("clik", tm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testConsumerFailoverWithConnectionStopped() throws Exception
   {
      Connection conn = null;

      try
      {
         // skip connection to node 0
         conn = cf.createConnection();
         conn.close();

         // create a connection to node 1
         conn = cf.createConnection();

         assertEquals(1, ((JBossConnection)conn).getServerID());

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue[1]);
         MessageProducer prod = session.createProducer(queue[1]);

         // send a message (connection is stopped, so it will stay on the server), and I expect
         // to receive it with the failed-over consumer after crash

         Message m = session.createTextMessage("plik");
         prod.send(m);


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

         // activate the failed-over consumer
         conn.start();

         TextMessage rm = (TextMessage)cons.receive(2000);
         assertNotNull(rm);
         assertEquals("plik", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testConsumerFailoverWithConnectionStarted() throws Exception
   {
      Connection conn = null;

      try
      {
         // skip connection to node 0
         conn = cf.createConnection();
         conn.close();

         // create a connection to node 1
         conn = cf.createConnection();

         assertEquals(1, ((JBossConnection)conn).getServerID());

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue[1]);
         MessageProducer prod = session.createProducer(queue[1]);

         // start the connection, so the message makes it to the client-side MessageCallbackHandler
         // buffer

         conn.start();

         Message m = session.createTextMessage("nik");
         prod.send(m);

         // wait a bit so the message makes it to the client
         log.info("sleeping 2 secs ...");
         Thread.sleep(2000);

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

         TextMessage rm = (TextMessage)cons.receive(2000);
         assertNotNull(rm);
         assertEquals("nik", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testBrowserFailoverSendMessagesPreFailure() throws Exception
   {
      Connection conn = null;

      try
      {
         // skip connection to node 0
         conn = cf.createConnection();
         conn.close();

         // create a connection to node 1
         conn = cf.createConnection();

         assertEquals(1, ((JBossConnection)conn).getServerID());

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         QueueBrowser browser = session.createBrowser(queue[1]);

         Enumeration en = browser.getEnumeration();
         assertFalse(en.hasMoreElements());

         // send one persistent and one non-persistent message

         MessageProducer prod = session.createProducer(queue[1]);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod.send(session.createTextMessage("click"));
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         prod.send(session.createTextMessage("clack"));

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

         en = browser.getEnumeration();

         // we expect to only be able to browse the persistent message
         assertTrue(en.hasMoreElements());
         TextMessage tm = (TextMessage)en.nextElement();
         assertEquals("click", tm.getText());

         assertFalse(en.hasMoreElements());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * TODO - Must double check if this is desired browser behavior - currently, once
    *        getEnumeration() was called once, all subsequent getEnumeration() calls return
    *        the same depleted iterator.
    */
   public void testBrowserFailoverSendMessagesPostFailure() throws Exception
   {
      Connection conn = null;

      try
      {
         // skip connection to node 0
         conn = cf.createConnection();
         conn.close();

         // create a connection to node 1
         conn = cf.createConnection();

         assertEquals(1, ((JBossConnection)conn).getServerID());

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         QueueBrowser browser = session.createBrowser(queue[1]);

         Enumeration en = browser.getEnumeration();
         assertFalse(en.hasMoreElements());

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

         // send one persistent and one non-persistent message

         MessageProducer prod = session.createProducer(queue[1]);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod.send(session.createTextMessage("click"));
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         prod.send(session.createTextMessage("clack"));

         en = browser.getEnumeration();

         // we expect to be able to browse persistent and non-persistent messages
         Set texts = new HashSet();

         assertTrue(en.hasMoreElements());
         TextMessage tm = (TextMessage)en.nextElement();
         texts.add(tm.getText());

         assertTrue(en.hasMoreElements());
         tm = (TextMessage)en.nextElement();
         texts.add(tm.getText());

         assertFalse(en.hasMoreElements());

         assertTrue(texts.contains("click"));
         assertTrue(texts.contains("clack"));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Sending one persistent message.
    */
   public void testSessionWithOneTransactedPersistentMessageFailover() throws Exception
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

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         // send 2 transacted messages (one persistent and one non-persistent) but don't commit
         MessageProducer prod = session.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod.send(session.createTextMessage("clik-persistent"));

         // close the producer
         prod.close();

         log.debug("producer closed");

         // create a consumer on the same local queue (creating a consumer AFTER failover will end
         // up getting messages from a local queue, not a failed over queue; at least until
         // redistribution is implemented.

         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session2.createConsumer(queue[1]);

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

         // commit the failed-over session
         session.commit();

         // make sure messages made it to the queue

         TextMessage tm = (TextMessage)cons.receive(2000);
         assertNotNull(tm);
         assertEquals("clik-persistent", tm.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Sending one non-persistent message.
    */
   public void testSessionWithOneTransactedNonPersistentMessageFailover() throws Exception
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

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         // send 2 transacted messages (one persistent and one non-persistent) but don't commit
         MessageProducer prod = session.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         prod.send(session.createTextMessage("clik-non-persistent"));

         // close the producer
         prod.close();

         log.debug("producer closed");

         // create a consumer on the same local queue (creating a consumer AFTER failover will end
         // up getting messages from a local queue, not a failed over queue; at least until
         // redistribution is implemented.

         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session2.createConsumer(queue[1]);

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

         // commit the failed-over session
         session.commit();

         // make sure messages made it to the queue

         TextMessage tm = (TextMessage)cons.receive(2000);
         assertNotNull(tm);
         assertEquals("clik-non-persistent", tm.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Sending 2 non-persistent messages.
    */
   public void testSessionWithTwoTransactedNonPersistentMessagesFailover() throws Exception
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

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         // send 2 transacted messages (one persistent and one non-persistent) but don't commit
         MessageProducer prod = session.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         prod.send(session.createTextMessage("clik-non-persistent"));
         prod.send(session.createTextMessage("clak-non-persistent"));

         // close the producer
         prod.close();

         log.debug("producer closed");

         // create a consumer on the same local queue (creating a consumer AFTER failover will end
         // up getting messages from a local queue, not a failed over queue; at least until
         // redistribution is implemented.

         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session2.createConsumer(queue[1]);

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

         // commit the failed-over session
         session.commit();

         // make sure messages made it to the queue

         TextMessage tm = (TextMessage)cons.receive(2000);
         assertNotNull(tm);
         assertEquals("clik-non-persistent", tm.getText());

         tm = (TextMessage)cons.receive(2000);
         assertNotNull(tm);
         assertEquals("clak-non-persistent", tm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Sending 2 persistent messages.
    */
   public void testSessionWithTwoTransactedPersistentMessagesFailover() throws Exception
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

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         // send 2 transacted messages (one persistent and one non-persistent) but don't commit
         MessageProducer prod = session.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod.send(session.createTextMessage("clik-persistent"));
         prod.send(session.createTextMessage("clak-persistent"));

         // close the producer
         prod.close();

         log.debug("producer closed");

         // create a consumer on the same local queue (creating a consumer AFTER failover will end
         // up getting messages from a local queue, not a failed over queue; at least until
         // redistribution is implemented.

         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session2.createConsumer(queue[1]);

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

         // commit the failed-over session
         session.commit();

         // make sure messages made it to the queue

         TextMessage tm = (TextMessage)cons.receive(2000);
         assertNotNull(tm);
         assertEquals("clik-persistent", tm.getText());

         tm = (TextMessage)cons.receive(2000);
         assertNotNull(tm);
         assertEquals("clak-persistent", tm.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Sending a mix of persistent and non-persistent messages.
    */
   public void testSessionWithTwoTransactedMixedMessagesFailover() throws Exception
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

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         // send 2 transacted messages (one persistent and one non-persistent) but don't commit
         MessageProducer prod = session.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         prod.send(session.createTextMessage("clik-non-persistent"));

         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod.send(session.createTextMessage("clak-persistent"));

         // close the producer
         prod.close();

         log.debug("producer closed");

         // create a consumer on the same local queue (creating a consumer AFTER failover will end
         // up getting messages from a local queue, not a failed over queue; at least until
         // redistribution is implemented.

         Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session2.createConsumer(queue[1]);

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

         // commit the failed-over session
         session.commit();

         // make sure messages made it to the queue

         TextMessage tm = (TextMessage)cons.receive(2000);
         assertNotNull(tm);
         assertEquals("clik-non-persistent", tm.getText());

         tm = (TextMessage)cons.receive(2000);
         assertNotNull(tm);
         assertEquals("clak-persistent", tm.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testSessionWithAcknowledgmentsFailover() throws Exception
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

         Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         // send 2 messages (one persistent and one non-persistent)

         MessageProducer prod = session.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod.send(session.createTextMessage("clik-persistent"));
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         prod.send(session.createTextMessage("clak-non-persistent"));

         // close the producer
         prod.close();

         // create a consumer and receive messages, but don't acknowledge

         MessageConsumer cons = session.createConsumer(queue[1]);
         TextMessage clik = (TextMessage)cons.receive(2000);
         assertEquals("clik-persistent", clik.getText());
         TextMessage clak = (TextMessage)cons.receive(2000);
         assertEquals("clak-non-persistent", clak.getText());

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

         // acknowledge the messages
         clik.acknowledge();
         clak.acknowledge();

         // make sure no messages are left in the queue
         Message m = cons.receive(1000);
         assertNull(m);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testTransactedSessionWithAcknowledgmentsCommitOnFailover() throws Exception
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

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         // send 2 messages (one persistent and one non-persistent)

         MessageProducer prod = session.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod.send(session.createTextMessage("clik-persistent"));
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         prod.send(session.createTextMessage("clak-non-persistent"));

         session.commit();

         // close the producer
         prod.close();

         // create a consumer and receive messages, but don't acknowledge

         MessageConsumer cons = session.createConsumer(queue[1]);
         TextMessage clik = (TextMessage)cons.receive(2000);
         assertEquals("clik-persistent", clik.getText());
         TextMessage clak = (TextMessage)cons.receive(2000);
         assertEquals("clak-non-persistent", clak.getText());

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

         // acknowledge the messages
         session.commit();

         // make sure no messages are left in the queue
         Message m = cons.receive(1000);
         assertNull(m);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testTransactedSessionWithAcknowledgmentsRollbackOnFailover() throws Exception
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

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         // send 2 messages (one persistent and one non-persistent)

         MessageProducer prod = session.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         prod.send(session.createTextMessage("clik-persistent"));
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         prod.send(session.createTextMessage("clak-non-persistent"));

         session.commit();

         // close the producer
         prod.close();

         // create a consumer and receive messages, but don't acknowledge

         MessageConsumer cons = session.createConsumer(queue[1]);
         TextMessage clik = (TextMessage)cons.receive(2000);
         assertEquals("clik-persistent", clik.getText());
         TextMessage clak = (TextMessage)cons.receive(2000);
         assertEquals("clak-non-persistent", clak.getText());

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

         session.rollback();

         TextMessage m = (TextMessage)cons.receive(2000);
         assertNotNull(m);
         assertEquals("clik-persistent", m.getText());

         m = (TextMessage)cons.receive(2000);
         assertNull(m);
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

   public void testFailoverMessageOnServer2() throws Exception
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

         // send a message

         prod.send(session.createTextMessage("blip"));

         // kill node 1

         log.debug("killing node 1");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // wait until the failure (not the completion of client-side failover) is detected

         assertEquals(FailoverEvent.FAILURE_DETECTED, listener.getEvent(60000).getType());

         // create a consumer the very next moment the failure is detected. This way, we also
         // test the client-side failover valve

         MessageConsumer cons = session.createConsumer(queue[0]);

         // we must receive the message

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

   public void testSimpleFailover() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         conn.close();

         conn = cf.createConnection();
         conn.start();

         // create a producer/consumer on node 1

         // make sure we're connecting to node 1

         int nodeID = ((ConnectionState)((DelegateSupport)((JBossConnection)conn).
            getDelegate()).getState()).getServerID();

         assertEquals(1, nodeID);

         Session s1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c1 = s1.createConsumer(queue[1]);
         MessageProducer p1 = s1.createProducer(queue[1]);
         p1.setDeliveryMode(DeliveryMode.PERSISTENT);

         // send a message

         p1.send(s1.createTextMessage("blip"));

         // kill node 1


         ServerManagement.killAndWait(1);
         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         try
         {
            ic[1].lookup("queue"); // looking up anything
            fail("The server still alive, kill didn't work yet");
         }
         catch (Exception e)
         {
         }

         // we must receive the message

         TextMessage tm = (TextMessage)c1.receive(1000);
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
