package org.hornetq.tests.integration.client;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnection;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;

public class AddressOOME implements MessageListener
{

   public static HornetQSession coreSession;

   public static void main(String[] args) throws Exception
   {

      final String queueName = "request-reply-queue";

      ConfigurationImpl conf = new ConfigurationImpl();

      final TransportConfiguration serverConfiguration = new TransportConfiguration(InVMAcceptorFactory.class.getName());
      final TransportConfiguration clientConfiguration = new TransportConfiguration(InVMConnectorFactory.class.getName());
      conf.getAcceptorConfigurations().add(serverConfiguration);
      conf.setSecurityEnabled(false);

      HornetQServer server = HornetQ.newHornetQServer(conf, false);
      JMSServerManager serverManager = new JMSServerManagerImpl(server);
      serverManager.setContext(null);
      serverManager.start();
      serverManager.createQueue(queueName, null, null, false);

      MessageProducer controlProducer = init(queueName, clientConfiguration, new AddressOOME());

      try
      {
         int count = 0;
         while (true)
         {
            HornetQMessage message = (HornetQMessage)coreSession.createTextMessage("myMessageWithNumber[" + (count++) +
                                                                                     "]");
            TemporaryQueue replyQueue = coreSession.createTemporaryQueue();
            message.setJMSReplyTo(replyQueue);
            controlProducer.send(message);
            MessageConsumer temporaryConsumer = coreSession.createConsumer(replyQueue);
            TextMessage returnMessage = (TextMessage)temporaryConsumer.receive(3000);
            if (returnMessage != null)
            {
               System.out.println("received answer: [" + returnMessage.getText() + "]");
            }
            else
            {
               System.out.println("timeout on receiveing answer");
            }
            temporaryConsumer.close();
            replyQueue.delete();
         }
      }
      catch (JMSException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
         return;
      }

   }

   private static MessageProducer init(final String queueName,
                                       final TransportConfiguration transportConfiguration,
                                       MessageListener listener) throws JMSException
   {

      MessageProducer controlProducer;
      HornetQConnectionFactory cf = new HornetQConnectionFactory(transportConfiguration);

      HornetQConnection connection = (HornetQConnection)cf.createQueueConnection();
      connection.start();
      coreSession = (HornetQSession)connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = coreSession.createQueue(queueName);
      controlProducer = coreSession.createProducer(queue);
      coreSession.start();
      MessageConsumer consumer = coreSession.createConsumer(queue);
      consumer.setMessageListener(listener);

      return controlProducer;
   }

   public void onMessage(Message msg)
   {
      TextMessage message = (TextMessage)msg;
      try
      {
         String textMessage = (String)message.getText();
         Destination replyDestination = msg.getJMSReplyTo();
         MessageProducer replyProducer = coreSession.createProducer(replyDestination);
         HornetQMessage replyMessage = (HornetQMessage)coreSession.createTextMessage(textMessage + "_ANSWERED");
         replyProducer.send(replyMessage);
         replyProducer.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

}