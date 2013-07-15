/**
 *
 */
package org.hornetq.tests.integration.jms;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.jms.Destination;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;

import org.hornetq.tests.integration.jms.JmsProducerCompletionListenerTest.CountingCompletionListener;
import org.hornetq.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JmsContextTest extends JMSTestBase
{

   private JMSContext context;
   private final Random random = new Random();
   private Queue queue1;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      context = createContext();
      queue1 = createQueue(JmsContextTest.class.getSimpleName() + "Queue");
   }

   @Test
   public void testCreateContext()
   {
      Assert.assertNotNull(context);
   }

   @Test
   public void testInvalidMessage()
   {
      JMSProducer producer = context.createProducer();
      try
      {
         producer.send(queue1, (Message)null);
         Assert.fail("null msg");
      }
      catch (MessageFormatRuntimeException expected)
      {
         // no-op
      }
   }

   @Test
   public void testInvalidDestination()
   {
      JMSProducer producer = context.createProducer();
      Message msg = context.createMessage();
      try
      {
         producer.send((Destination)null, msg);
         Assert.fail("null Destination");
      }
      catch (InvalidDestinationRuntimeException expected)
      {
         // no-op
      }
   }

   @Test
   public void testSendStreamMessage() throws JMSException, InterruptedException
   {
      CountingCompletionListener cl = new CountingCompletionListener(1);
      JMSProducer producer = context.createProducer();
      producer.setAsync(cl);
      StreamMessage msg = context.createStreamMessage();
      msg.setStringProperty("name", name.getMethodName());
      String bprop = "booleanProp";
      String iprop = "intProp";
      msg.setBooleanProperty(bprop, true);
      msg.setIntProperty(iprop, 42);
      msg.writeBoolean(true);
      msg.writeInt(67);
      producer.send(queue1, msg);
      JMSConsumer consumer = context.createConsumer(queue1);
      Message msg2 = consumer.receive(100);
      Assert.assertNotNull(msg2);
      Assert.assertTrue(cl.completionLatch.await(1, TimeUnit.SECONDS));
      StreamMessage sm = (StreamMessage)cl.lastMessage;
      Assert.assertEquals(true, sm.getBooleanProperty(bprop));
      Assert.assertEquals(42, sm.getIntProperty(iprop));
      Assert.assertEquals(true, sm.readBoolean());
      Assert.assertEquals(67, sm.readInt());
   }

   @Test
   public void testSetClientIdLate()
   {
      JMSProducer producer = context.createProducer();
      Message msg = context.createMessage();
      producer.send(queue1, msg);
      try
      {
         context.setClientID("id");
         Assert.fail("expected exception");
      }
      catch (IllegalStateRuntimeException e)
      {
         // no op
      }
   }

   @Test
   public void testCloseSecondContextConnectionRemainsOpen() throws JMSException
   {
      JMSContext localContext = context.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
      Assert.assertEquals("client_ack", JMSContext.CLIENT_ACKNOWLEDGE, localContext.getSessionMode());
      JMSProducer producer = localContext.createProducer();
      JMSConsumer consumer = localContext.createConsumer(queue1);

      final int pass = 1;
      for (int idx = 0; idx < 2; idx++)
      {
         Message m = localContext.createMessage();
         int intProperty = random.nextInt();
         m.setIntProperty("random", intProperty);
         Assert.assertNotNull(m);
         producer.send(queue1, m);
         m = null;
         Message msg = consumer.receive(100);
         Assert.assertNotNull("must have a msg", msg);
         Assert.assertEquals(intProperty, msg.getIntProperty("random"));
         /* In the second pass we close the connection before ack'ing */
         if (idx == pass)
         {
            localContext.close();
         }
         /**
          * From {@code JMSContext.close()}'s javadoc:<br/>
          * Invoking the {@code acknowledge} method of a received message from a closed connection's
          * session must throw an {@code IllegalStateRuntimeException}. Closing a closed connection
          * must NOT throw an exception.
          */
         try {
            msg.acknowledge();
            Assert.assertEquals("connection should be open on pass 0. It is " + pass, 0, idx);
         }
         // HORNETQ-1209 "JMS 2.0" XXX JMSContext javadoc says we must expect a
         // IllegalStateRuntimeException here. But Message.ack...() says it must throws the
         // non-runtime variant.
         catch (javax.jms.IllegalStateException expected)
         {
            Assert.assertEquals("we only close the connection on pass " + pass, pass, idx);
         }
      }
   }

   @Test(expected = JMSRuntimeException.class)
   public void testInvalidSessionModesValueMinusOne()
   {
      context.createContext(-1);
   }

   @Test(expected = JMSRuntimeException.class)
   public void testInvalidSessionModesValue4()
   {
      context.createContext(4);
   }

   @Test
   public void testGetAnotherContextFromIt()
   {
      JMSContext c2 = context.createContext(Session.DUPS_OK_ACKNOWLEDGE);
      Assert.assertNotNull(c2);
      Assert.assertEquals(Session.DUPS_OK_ACKNOWLEDGE, c2.getSessionMode());
      Message m2 = c2.createMessage();
      Assert.assertNotNull(m2);
      c2.close(); // should close its session, but not its (shared) connection
      try
      {
         c2.createMessage();
         Assert.fail("session should be closed...");
      }
      catch (JMSRuntimeException expected)
      {
         // expected
      }
      Message m1 = context.createMessage();
      Assert.assertNotNull("connection must be open", m1);
   }

   @Test
   public void testSetGetClientIdNewContext()
   {
      final String id = "123";
      JMSContext c = context;// createContext();
      c.setClientID(id);
      JMSContext c2 = addContext(c.createContext(Session.CLIENT_ACKNOWLEDGE));
      Assert.assertEquals(id, c2.getClientID());
   }

   @Test
   public void testGetClientId()
   {
      JMSContext context2 = addContext(context.createContext(Session.AUTO_ACKNOWLEDGE));
      final String id = "ID: " + random.nextInt();
      context.setClientID(id);
      Assert.assertEquals("id's must match because the connection is shared", id, context2.getClientID());
   }

   @Test
   public void testCreateConsumerWithSelector() throws JMSException
   {
      final String filterName = "magicIndexMessage";
      final int total = 5;
      JMSProducer producer = context.createProducer();
      JMSConsumer consumerNoSelect = context.createConsumer(queue1);
      JMSConsumer consumer = context.createConsumer(queue1, filterName + "=TRUE");
      for (int i = 0; i < total; i++)
      {
         Message msg = context.createTextMessage("message " + i);
         msg.setBooleanProperty(filterName, i == 3);
         producer.send(queue1, msg);
      }
      Message msg0 = consumer.receive(500);
      Assert.assertNotNull(msg0);
      msg0.acknowledge();
      Assert.assertNull("no more messages", consumer.receiveNoWait());
      for (int i = 0; i < total - 1; i++)
      {
         Message msg = consumerNoSelect.receive(100);
         Assert.assertNotNull(msg);
         msg.acknowledge();
      }
      Assert.assertNull("no more messages", consumerNoSelect.receiveNoWait());
   }
}
