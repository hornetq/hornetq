package org.hornetq.tests.integration.stomp;

import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.protocol.stomp.Stomp;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.impl.JMSConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.hornetq.jms.server.config.impl.TopicConfigurationImpl;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.tests.unit.util.InVMContext;

public class ExtraStompTest extends StompTestBase
{
   public ExtraStompTest()
   {
      autoCreateServer = false;
   }

   public void testConnectionTTL() throws Exception
   {
      try
      {
         server = createServerWithTTL(3000);
         server.start();

         setUpAfterServer();

         String connect_frame = "CONNECT\n" + "login: brianm\n"
               + "passcode: wombats\n" + "request-id: 1\n" + "\n" + Stomp.NULL;
         sendFrame(connect_frame);

         String f = receiveFrame(10000);
         Assert.assertTrue(f.startsWith("CONNECTED"));
         Assert.assertTrue(f.indexOf("response-id:1") >= 0);
         
         String frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + "Hello World 1" + Stomp.NULL;
         sendFrame(frame);
         
         //sleep to let the connection die
         Thread.sleep(8000);
         
         frame = "SEND\n" + "destination:" + getQueuePrefix() + getQueueName() + "\n\n" + "Hello World 2" + Stomp.NULL;
         
         try
         {
            sendFrame(frame);
            fail("Message has been sent successfully after ttl expires! ttl configuration not working!");
         }
         catch (SocketException e)
         {
            //expected.
         }

         MessageConsumer consumer = session.createConsumer(queue);

         TextMessage message = (TextMessage)consumer.receive(1000);
         Assert.assertNotNull(message);
         
         message = (TextMessage)consumer.receive(2000);
         Assert.assertNull(message);
      }
      finally
      {
         cleanUp();
         server.stop();
      }
   }
   
   protected JMSServerManager createServerWithTTL(long ttl) throws Exception
   {
      Configuration config = createBasicConfig();
      config.setSecurityEnabled(false);
      config.setPersistenceEnabled(false);

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PROTOCOL_PROP_NAME, ProtocolType.STOMP.toString());
      params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_STOMP_PORT);
      params.put(TransportConstants.CONNECTION_TTL, ttl);
      params.put(TransportConstants.STOMP_CONSUMERS_CREDIT, "-1");
      TransportConfiguration stompTransport = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);
      config.getAcceptorConfigurations().add(stompTransport);
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      HornetQServer hornetQServer = HornetQServers.newHornetQServer(config, defUser, defPass);

      JMSConfiguration jmsConfig = new JMSConfigurationImpl();
      jmsConfig.getQueueConfigurations()
               .add(new JMSQueueConfigurationImpl(getQueueName(), null, false, getQueueName()));
      jmsConfig.getTopicConfigurations().add(new TopicConfigurationImpl(getTopicName(), getTopicName()));
      server = new JMSServerManagerImpl(hornetQServer, jmsConfig);
      server.setContext(new InVMContext());
      return server;
   }

}
