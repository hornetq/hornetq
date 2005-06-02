/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import java.util.Enumeration;
import java.util.HashSet;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.util.InVMInitialContextFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Session;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.naming.InitialContext;

/**
 * Tests for different types of message
 *
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public class MessageBodyTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Destination queue;
   protected Connection producerConnection, consumerConnection;
   protected Session queueProducerSession, queueConsumerSession;
   protected MessageProducer queueProducer;
   protected MessageConsumer queueConsumer;

   // Constructors --------------------------------------------------

   public MessageBodyTest(String name)
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

      queueProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProducer = queueProducerSession.createProducer(queue);
      queueConsumer = queueConsumerSession.createConsumer(queue);

      consumerConnection.start();
   }

   public void tearDown() throws Exception
   {
      // TODO uncomment these
      producerConnection.close();
      consumerConnection.close();

      ServerManagement.undeployQueue("Queue");
      ServerManagement.stopInVMServer();

      super.tearDown();
   }
   
   public void testBytesMessage() throws Exception
   {
   	BytesMessage m = queueProducerSession.createBytesMessage();
   	
   	boolean myBool = true;
   	byte myByte = Byte.MAX_VALUE;
   	short myShort = Short.MAX_VALUE;
   	int myInt = Integer.MAX_VALUE;
   	long myLong = Long.MAX_VALUE;
   	float myFloat = Float.MAX_VALUE;
   	double myDouble = Double.MAX_VALUE;
   	String myString = "abcdefghijkl";
   	
   	m.writeBoolean(myBool);
   	m.writeByte(myByte);

   	
   	
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
