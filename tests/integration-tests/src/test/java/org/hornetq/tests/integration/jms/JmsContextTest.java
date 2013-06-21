/**
 *
 */
package org.hornetq.tests.integration.jms;

import java.util.Random;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Session;

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

   @Test(expected = IllegalArgumentException.class)
   public void testInvalidSessionModesValueMinusOne()
   {
      context.createContext(-1);
   }

   @Test(expected = IllegalArgumentException.class)
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
}
