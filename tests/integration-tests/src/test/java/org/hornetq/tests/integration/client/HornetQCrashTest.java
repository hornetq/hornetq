package org.hornetq.tests.integration.client;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.SendAcknowledgementHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * From https://jira.jboss.org/jira/browse/HORNETQ-144
 * 
 */
public class HornetQCrashTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(HornetQCrashTest.class);

   public HornetQServer server;

   private volatile boolean ackReceived;
   private ServerLocator locator;

   public void testHang() throws Exception
   {
      Configuration configuration = createDefaultConfig();
      configuration.setPersistenceEnabled(false);
      configuration.setSecurityEnabled(false);
      configuration.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      server = HornetQServers.newHornetQServer(configuration);

      server.start();

      server.getRemotingService().addInterceptor(new AckInterceptor(server));




      // Force an ack at once - this means the send call will block
      locator.setConfirmationWindowSize(1);

      ClientSessionFactory clientSessionFactory = locator.createSessionFactory();

      ClientSession session = clientSessionFactory.createSession();

      session.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         public void sendAcknowledged(final Message message)
         {
            ackReceived = true;
         }
      });

      ClientProducer producer = session.createProducer("fooQueue");

      ClientMessage msg = session.createMessage(false);

      msg.putStringProperty("someKey", "someValue");

      producer.send(msg);

      Thread.sleep(250);

      Assert.assertFalse(ackReceived);

      session.close();
   }

   public static class AckInterceptor implements Interceptor
   {
      private final HornetQServer server;

      AckInterceptor(final HornetQServer server)
      {
         this.server = server;
      }

      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         HornetQCrashTest.log.info("AckInterceptor.intercept " + packet);

         if (packet.getType() == PacketImpl.SESS_SEND)
         {
            try
            {
               HornetQCrashTest.log.info("Stopping server");

               new Thread()
               {
                  public void run()
                  {
                     try
                     {
                        server.stop();
                     }
                     catch (Exception e)
                     {
                        e.printStackTrace();
                     }
                  }
               }.start();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }

            return false;
         }
         return true;
      }

   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();


      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();

      super.tearDown();

      server = null;
   }
}
