/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.stress;

import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.logging.Logger;

import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Message;
import javax.jms.DeliveryMode;

/**
 * A stress test written to investigate http://jira.jboss.org/jira/browse/JBMESSAGING-362
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class CorruptMessageStressTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   private static Logger log = Logger.getLogger(CorruptMessageStressTest.class);

   public static int PRODUCER_COUNT = 30;
   public static int MESSAGE_COUNT = 5000;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContext ic;

   // Constructors --------------------------------------------------

   public CorruptMessageStressTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testMultipleSenders() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/StressTestQueue");
      drainDestination(cf, queue);

      Connection conn = cf.createConnection();

      Session[] sessions = new Session[PRODUCER_COUNT];
      MessageProducer[] producers = new MessageProducer[PRODUCER_COUNT];

      for(int i = 0; i < PRODUCER_COUNT; i++)
      {
         sessions[i] = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         producers[i] = sessions[i].createProducer(queue);
         producers[i].setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      }

      Thread[] threads = new Thread[PRODUCER_COUNT];

      for(int i = 0; i < PRODUCER_COUNT; i++)
      {
         threads[i] = new Thread(new Sender(sessions[i], producers[i]), "Sender Thread #" + i);
         threads[i].start();
         log.info(threads[i].getName() + " started");
      }

      // wait for the threads to finish

      for(int i = 0; i < PRODUCER_COUNT; i++)
      {
         threads[i].join();
      }

      conn.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");
      ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ServerManagement.deployQueue("StressTestQueue");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("StressTestQueue");
      ic.close();
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class Sender implements Runnable
   {
      private Session session;
      private MessageProducer producer;
      private int count = 0;

      public Sender(Session session, MessageProducer producer)
      {
         this.session = session;
         this.producer = producer;
      }

      public void run()
      {
         while(true)
         {
            if (count == MESSAGE_COUNT)
            {
               log.info(Thread.currentThread().getName() + " done");
               break;
            }

            try
            {
               Message m = session.createMessage();
               m.setStringProperty("XXX", "XXX-VALUE");
               m.setStringProperty("YYY", "YYY-VALUE");
               producer.send(m);
               count++;
            }
            catch(Exception e)
            {
               log.error("Sender thread failed", e);
               break;
            }
         }
      }
   }
}
