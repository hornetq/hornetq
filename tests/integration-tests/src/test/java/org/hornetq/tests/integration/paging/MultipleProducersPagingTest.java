/**
 *
 */
package org.hornetq.tests.integration.paging;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.hornetq.jms.server.embedded.EmbeddedJMS;
import org.hornetq.jms.tests.HornetQServerTestCase;
import org.hornetq.tests.util.UnitTestCase;

public class MultipleProducersPagingTest extends UnitTestCase
{
   private static final int CONSUMER_WAIT_TIME_MS = 250;
   private static final int PRODUCERS = 5;
   private static final long MESSAGES_PER_PRODUCER = 2000;
   private static final long TOTAL_MSG = MESSAGES_PER_PRODUCER * PRODUCERS;
   private ExecutorService executor;
   private CountDownLatch runnersLatch;
   private CyclicBarrier barrierLatch;

   private AtomicLong msgReceived;
   private AtomicLong msgSent;
   private final Set<Connection> connections=new HashSet<Connection>() ;
   private EmbeddedJMS jmsServer;
   private ConnectionFactory cf;
   private Queue queue;

   @Override
   @Before
   public void setUp() throws Exception
   {
      HornetQServerTestCase.tearDownAllServers();
      super.setUp();
      executor = Executors.newCachedThreadPool();

      Configuration config = createBasicConfig();

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.setPageSizeBytes(50000);
      addressSettings.setMaxSizeBytes(404850);
      config.setPersistenceEnabled(false);
      config.setJMXManagementEnabled(true);
      config.setAddressesSettings(Collections.singletonMap("#", addressSettings));
      config.setAcceptorConfigurations(Collections.singleton(new TransportConfiguration(
                                                                                        NettyAcceptorFactory.class.getName())));
      config.setConnectorConfigurations(Collections.singletonMap("netty",
                                                                 new TransportConfiguration(
                                                                                            NettyConnectorFactory.class.getName())));

      final JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getConnectionFactoryConfigurations().add(new ConnectionFactoryConfigurationImpl("cf", false,
                                                                                                Arrays.asList("netty"),
                                                                                                "/cf"));
      jmsConfig.getQueueConfigurations().add(new JMSQueueConfigurationImpl("simple", "", false, "/queue/simple"));

      jmsServer = new EmbeddedJMS();
      jmsServer.setConfiguration(config);
      jmsServer.setJmsConfiguration(jmsConfig);
      jmsServer.start();

      cf = (ConnectionFactory)jmsServer.lookup("/cf");
      queue = (Queue)jmsServer.lookup("/queue/simple");

      barrierLatch = new CyclicBarrier(PRODUCERS + 1);
      runnersLatch = new CountDownLatch(PRODUCERS + 1);
      msgReceived = new AtomicLong(0);
      msgSent = new AtomicLong(0);
   }


   @Test
   public void testQueue() throws InterruptedException
   {
      executor.execute(new ConsumerRun());
      for (int i = 0; i < PRODUCERS; i++)
      {
         executor.execute(new ProducerRun());
      }
      assertTrue("must take less than a minute to run", runnersLatch.await(1, TimeUnit.MINUTES));
      assertEquals("number sent", TOTAL_MSG, msgSent.longValue());
      assertEquals("number received", TOTAL_MSG, msgReceived.longValue());
   }

   private synchronized Session createSession() throws JMSException
   {
      Connection connection = cf.createConnection();
      connections.add(connection);
      connection.start();
      return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
   }

   final class ConsumerRun implements Runnable
   {

      @Override
      public void run()
      {
         try
         {
            Session session = createSession();
            MessageConsumer consumer = session.createConsumer(queue);
            barrierLatch.await();
            while (true)
            {
               Message msg = consumer.receive(CONSUMER_WAIT_TIME_MS);
               if (msg == null)
                  break;
               msgReceived.incrementAndGet();
            }
         }
         catch (Exception e)
         {
            throw new RuntimeException(e);
         }
         finally
         {
            runnersLatch.countDown();
         }
      }

   }

   final class ProducerRun implements Runnable
   {
      @Override
      public void run()
      {
         try
         {
            Session session = createSession();
            MessageProducer producer = session.createProducer(queue);
            barrierLatch.await();

            for (int i = 0; i < MESSAGES_PER_PRODUCER; i++)
            {
               producer.send(session.createTextMessage(this.hashCode() + " counter " + i));
               msgSent.incrementAndGet();
            }
         }
         catch (Exception cause)
         {
            throw new RuntimeException(cause);
         }
         finally
         {
            runnersLatch.countDown();
         }
      }
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      executor.shutdown();
      for (Connection conn : connections)
      {
         conn.close();
      }
      connections.clear();
      if (jmsServer != null)
         jmsServer.stop();
      super.tearDown();
   }
}
