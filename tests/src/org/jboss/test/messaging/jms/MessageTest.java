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
import javax.naming.InitialContext;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Base class for all tests concerning message headers, properties, etc.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Destination queue;
   protected Connection producerConnection, consumerConnection;
   protected Session producerSession, consumerSession;
   protected MessageProducer producer;
   protected MessageConsumer consumer;

   // Constructors --------------------------------------------------

   public MessageTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.startInVMServer();
      ServerManagement.deployQueue("Queue");

      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/messaging/ConnectionFactory");
      queue = (Destination)ic.lookup("/messaging/queues/Queue");

      producerConnection = cf.createConnection();
      consumerConnection = cf.createConnection();

      producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      producer = producerSession.createProducer(queue);
      consumer = consumerSession.createConsumer(queue);

      consumerConnection.start();
   }

   public void tearDown() throws Exception
   {
      // TODO uncomment these
//      producerConnection.close();
//      consumerConnection.close();

      ServerManagement.undeployQueue("Queue");
      ServerManagement.stopInVMServer();

      super.tearDown();
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
