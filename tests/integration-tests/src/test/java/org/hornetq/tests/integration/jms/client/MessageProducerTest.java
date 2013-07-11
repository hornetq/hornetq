/**
 *
 */
package org.hornetq.tests.integration.jms.client;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.NamingException;

import org.hornetq.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class MessageProducerTest extends JMSTestBase
{

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      conn = cf.createConnection();
   };

   @Test
   public void testNoDefaultDestination() throws JMSException
   {
      Session session = conn.createSession();
      try
      {
         MessageProducer producer = session.createProducer(null);
         Message m = session.createMessage();
         try
         {
            producer.send(m);
            Assert.fail("must not be reached");
         }
         catch (UnsupportedOperationException cause)
         {
            // expected
         }
      }
      finally
      {
         session.close();
      }
   }

   @Test
   public void testHasDefaultDestination() throws NamingException, Exception
   {
      Session session = conn.createSession();
      try
      {
         Queue queue = createQueue(name.getMethodName());
         Queue queue2 = createQueue(name.getMethodName() + "2");
         MessageProducer producer = session.createProducer(queue);
         Message m = session.createMessage();
         try
         {
            producer.send(queue2, m);
            Assert.fail("must not be reached");
         }
         catch (UnsupportedOperationException cause)
         {
            // expected
         }
         try
         {
            producer.send(queue, m);
            Assert.fail("tck7 requires an UnsupportedOperationException "
                     + "even if the destination is the same as the default one");
         }
         catch (UnsupportedOperationException cause)
         {
            // expected
         }
      }
      finally
      {
         session.close();
      }
   }
}
