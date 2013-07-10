/**
 *
 */
package org.hornetq.tests.integration.jms;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.CompletionListener;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;

import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.tests.util.JMSTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class JmsProducerCompletionListenerTest extends JMSTestBase
{
   static final int TOTAL_MSGS = 20;

   private JMSContext context;
   private JMSProducer producer;
   private Queue queue;

   private final int confirmationWindowSize;

   @Parameters(name = "confirmationWindowSize={0}")
   public static Iterable<Object[]> data()
   {
      return Arrays.asList(new Object[][] { { -1 }, { 0 }, { 10 }, { 1000 } });
   }

   public JmsProducerCompletionListenerTest(int confirmationWindowSize)
   {
      this.confirmationWindowSize = confirmationWindowSize;
   }

   @Override
   protected void testCaseCfExtraConfig(ConnectionFactoryConfiguration configuration)
   {
      configuration.setConfirmationWindowSize(confirmationWindowSize);
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      context = createContext();
      producer = context.createProducer();
      queue = createQueue(name.getMethodName() + "Queue");
   }

   @Test
   public void testCompletionListener() throws InterruptedException
   {
      CountingCompletionListener cl = new CountingCompletionListener(TOTAL_MSGS);
      Assert.assertEquals(null, producer.getAsync());
      producer.setAsync(cl);
      Assert.assertEquals(cl, producer.getAsync());
      producer.setAsync(null);
      producer.setAsync(cl);
      JMSConsumer consumer = context.createConsumer(queue);
      sendMessages(context, producer, queue, TOTAL_MSGS);
      receiveMessages(consumer, 0, TOTAL_MSGS, true);

      context.close();
      assertTrue("completion listener should be called", cl.completionLatch.await(3, TimeUnit.SECONDS));
   }

   public static final class CountingCompletionListener implements CompletionListener
   {

      public int completion;
      public int error;
      public CountDownLatch completionLatch;

      public CountingCompletionListener(int n)
      {
         completionLatch = new CountDownLatch(n);
      }

      @Override
      public void onCompletion(Message message)
      {
         completion++;
         completionLatch.countDown();
      }

      @Override
      public void onException(Message message, Exception exception)
      {
         error++;
      }

      @Override
      public String toString()
      {
         return JmsProducerCompletionListenerTest.class.getSimpleName() + ":" +
                  CountingCompletionListener.class.getSimpleName() + ":" + completionLatch;
      }
   }
}
