/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.util.InVMInitialContextFactory;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.DeliveryMode;
import javax.naming.InitialContext;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageProducerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Connection producerConnection, consumerConnection;
   protected Session producerSession, consumerSession;
   protected MessageProducer producer;
   protected MessageConsumer consumer;

   // Constructors --------------------------------------------------

   public MessageProducerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.startInVMServer();
      ServerManagement.deployTopic("Topic");

      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/messaging/ConnectionFactory");
      Destination topic = (Destination)ic.lookup("/messaging/topics/Topic");

      producerConnection = cf.createConnection();
      consumerConnection = cf.createConnection();

      producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      producer = producerSession.createProducer(topic);
      consumer = consumerSession.createConsumer(topic);

   }

   public void tearDown() throws Exception
   {
      // TODO uncomment these
//      producerConnection.close();
//      consumerConnection.close();

      ServerManagement.undeployTopic("Topic");
      ServerManagement.stopInVMServer();

      super.tearDown();
   }

   public void testDefaultDeliveryMode() throws Exception
   {
      assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
