/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.stress;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * A stress test written to investigate http://jira.jboss.org/jira/browse/JBMESSAGING-362
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class CorruptMessageStressTest extends StressTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public CorruptMessageStressTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
 
   public void testMultipleSenders() throws Exception
   {
      final int PRODUCER_COUNT = 30;
      final int MESSAGE_COUNT = 10000;
      
      Connection conn = cf.createConnection();
      
      Session[] sessions = new Session[PRODUCER_COUNT];
      MessageProducer[] producers = new MessageProducer[PRODUCER_COUNT];

      Runner[] runners = new Runner[PRODUCER_COUNT];
      
      for(int i = 0; i < PRODUCER_COUNT; i++)
      {
         sessions[i] = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         producers[i] = sessions[i].createProducer(queue1);
         producers[i].setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         runners[i] = new Sender("prod1", sessions[i], producers[i], MESSAGE_COUNT);
      }
      
      runRunners(runners);
      
      conn.close();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
  
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
