package org.hornetq.tests.integration.server;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.logging.AssertionLoggerHandler;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AddressFullLoggingTest extends ServiceTestBase
{
   @BeforeClass
   public static void prepareLogger()
   {
      AssertionLoggerHandler.startCapture();
   }

   @Test
   /**
    * When running this test from an IDE add this to the test command line so that the AssertionLoggerHandler works properly:
    *
    *   -Djava.util.logging.manager=org.jboss.logmanager.LogManager  -Dlogging.configuration=file:<path_to_source>/tests/config/logging.properties
    */
   public void testBlockLogging() throws Exception
   {
      final int MAX_MESSAGES = 200;
      final String MY_ADDRESS = "myAddress";
      final String MY_QUEUE = "myQueue";

      HornetQServer server = createServer(false);

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);
      defaultSetting.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);
      server.getAddressSettingsRepository().addMatch("#", defaultSetting);
      server.start();

      ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(MY_ADDRESS, MY_QUEUE, true);

      final ClientProducer producer = session.createProducer(MY_ADDRESS);

      final ClientMessage message = session.createMessage(false);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      ExecutorService executor = Executors.newFixedThreadPool(1);
      Callable<Object> sendMessageTask = new Callable<Object>()
      {
         public Object call() throws HornetQException
         {
            producer.send(message);
            return null;
         }
      };

      int sendCount = 0;

      for (int i = 0; i < MAX_MESSAGES; i++)
      {
         Future<Object> future = executor.submit(sendMessageTask);
         try
         {
            future.get(3, TimeUnit.SECONDS);
            sendCount++;
         }
         catch (TimeoutException ex)
         {
            // message sending has been blocked
            break;
         }
         finally
         {
            future.cancel(true); // may or may not desire this
         }
      }

      executor.shutdown();
      session.close();

      session = factory.createSession(false, true, true);
      session.start();
      ClientConsumer consumer = session.createConsumer(MY_QUEUE);
      for (int i = 0; i < sendCount; i++)
      {
         ClientMessage msg = consumer.receive(250);
         if (msg == null)
            break;
         msg.acknowledge();
      }

      session.close();
      locator.close();
      server.stop();

      // Using the code only so the test doesn't fail just because someone edits the log text
      Assert.assertTrue("Expected to find HQ222183", AssertionLoggerHandler.findText("HQ222183"));
      Assert.assertTrue("Expected to find HQ221046", AssertionLoggerHandler.findText("HQ221046"));
   }

   @AfterClass
   public static void clearLogger()
   {
      AssertionLoggerHandler.stopCapture();
   }
}
