/**
 *
 */
package org.hornetq.jms.tests.message;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.hornetq.jms.tests.HornetQServerTestCase;
import org.junit.After;
import org.junit.Before;

/**
 *
 */
public abstract class MessageBodyTestCase extends HornetQServerTestCase
{
   protected Connection producerConnection, consumerConnection;
   protected Session queueProducerSession, queueConsumerSession;
   protected MessageProducer queueProducer;
   protected MessageConsumer queueConsumer;

   enum JmsMessageType
   {
      TEXT, MAP, OBJECT, BYTE, STREAM;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      producerConnection = getConnectionFactory().createConnection();
      consumerConnection = getConnectionFactory().createConnection();

      queueProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProducer = queueProducerSession.createProducer(HornetQServerTestCase.queue1);
      queueConsumer = queueConsumerSession.createConsumer(HornetQServerTestCase.queue1);

      consumerConnection.start();
   }

   @After
   public void tearDown() throws Exception
   {
      producerConnection.close();
      consumerConnection.close();
   }

}
