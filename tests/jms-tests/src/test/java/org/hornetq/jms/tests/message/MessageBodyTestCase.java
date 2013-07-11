/**
 *
 */
package org.hornetq.jms.tests.message;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.hornetq.jms.tests.HornetQServerTestCase;
import org.junit.Before;

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

      producerConnection = addConnection(getConnectionFactory().createConnection());
      consumerConnection = addConnection(getConnectionFactory().createConnection());

      queueProducerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      queueProducer = queueProducerSession.createProducer(queue1);
      queueConsumer = queueConsumerSession.createConsumer(queue1);

      consumerConnection.start();
   }
}
