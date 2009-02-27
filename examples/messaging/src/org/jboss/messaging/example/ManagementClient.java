/*
 * JBoss, Home of Professional Open SourceCopyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package org.jboss.messaging.example;

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;
import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS;

import java.util.Set;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientRequestor;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.utils.SimpleString;

/*
 * Uses the core messaging API to send and receive a message to a queue.
 * 
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ManagementClient
{
   public static void main(final String[] args) throws Exception
   {
      SimpleString replytoQueue = new SimpleString("replyto.adminQueue");

      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory"));
      final ClientSession clientSession = sessionFactory.createSession(SecurityStoreImpl.CLUSTER_ADMIN_USER, ConfigurationImpl.DEFAULT_MANAGEMENT_CLUSTER_PASSWORD, false, true, true, false, 1);
      SimpleString queue = new SimpleString("queuejms.testQueue");

      sendMessages(clientSession, queue);

      // add temporary destination and queue
      clientSession.addDestination(replytoQueue, false, true);
      clientSession.createQueue(replytoQueue, replytoQueue, null, false, true);

      SimpleString notifQueue = new SimpleString("notifQueue");
      clientSession.createQueue(DEFAULT_MANAGEMENT_NOTIFICATION_ADDRESS, notifQueue, null, false, true);
      ClientConsumer notifConsumer = clientSession.createConsumer(notifQueue);
      notifConsumer.setMessageHandler(new MessageHandler()
      {

         public void onMessage(final ClientMessage message)
         {
            System.out.println("received notification" + message);
            Set<SimpleString> propertyNames = message.getPropertyNames();

            for (SimpleString key : propertyNames)
            {
               System.out.println(key + "=" + message.getProperty(key));
            }
            try
            {
               message.acknowledge();
            }
            catch (MessagingException e)
            {
               e.printStackTrace();
            }
         }

      });
      clientSession.start();

      // add and remove a destination to receive two notifications from the
      // server
      clientSession.addDestination(new SimpleString("anotherQueue"), false, true);
      clientSession.removeDestination(new SimpleString("anotherQueue"), false);

      ClientRequestor requestor = new ClientRequestor(clientSession, DEFAULT_MANAGEMENT_ADDRESS);

      // to set a new value for an attribute, invoke the corresponding setter
      // method
      ClientMessage mngmntMessage = clientSession.createClientMessage(false);
      ManagementHelper.putOperationInvocation(mngmntMessage,
                                              ObjectNames.getMessagingServerObjectName(),
                                              "setMessageCounterSamplePeriod",
                                              (long)30000);
      ClientMessage reply = requestor.request(mngmntMessage);
      System.out.println("sent management message to set an attribute");
      if (reply != null)
      {
         if (ManagementHelper.isOperationResult(reply))
         {
            System.out.println("\toperation succeeded:" + ManagementHelper.hasOperationSucceeded(reply));
            if (!ManagementHelper.hasOperationSucceeded(reply))
            {
               System.out.println("\t- exception=" + ManagementHelper.getOperationExceptionMessage(reply));
            }
         }
      }

      // create a message to retrieve one or many attributes
      mngmntMessage = clientSession.createClientMessage(false);
      ManagementHelper.putAttributes(mngmntMessage,
                                     ObjectNames.getQueueObjectName(queue, queue),
                                     "MessageCount",
                                     "Durable");
      reply = requestor.request(mngmntMessage);
      System.out.println("sent management message to retrieve attributes");
      if (reply != null)
      {
         System.out.println("\tattributes:");
         System.out.println("\t- MessageCount=" + reply.getProperty(new SimpleString("MessageCount")));
         System.out.println("\t- Durable=" + reply.getProperty(new SimpleString("Durable")));
      }

      // create a message to invoke the operation sendMessageToDeadLetterAddress(long) on the
      // queue
      mngmntMessage = clientSession.createClientMessage(false);
      ManagementHelper.putOperationInvocation(mngmntMessage,
                                              ObjectNames.getQueueObjectName(queue, queue),
                                              "sendMessageToDLQ",
                                              (long)6161);
      reply = requestor.request(mngmntMessage);
      System.out.println("sent management message to retrieve attributes");
      if (reply != null)
      {
         if (ManagementHelper.isOperationResult(reply))
         {
            System.out.println("\toperation succeeded:" + ManagementHelper.hasOperationSucceeded(reply));
            if (ManagementHelper.hasOperationSucceeded(reply))
            {
               System.out.println("\t- result=" + reply.getProperty(new SimpleString("sendMessageToDLQ")));
            }
            else
            {
               System.out.println("\t- exception=" + ManagementHelper.getOperationExceptionMessage(reply));
            }
         }
      }

      Thread.sleep(5000);

      consumeMessages(clientSession, queue);

      notifConsumer.close();
      clientSession.removeDestination(replytoQueue, false);
      clientSession.deleteQueue(replytoQueue);
      clientSession.deleteQueue(notifQueue);

      clientSession.close();
   }

   private static void consumeMessages(final ClientSession clientSession, final SimpleString queue) throws MessagingException
   {
      ClientConsumer clientConsumer = clientSession.createConsumer(queue);
      ClientMessage m = null;
      do
      {
         m = clientConsumer.receive(5000);
         if (m != null)
         {
            m.acknowledge();
         }
      }
      while (m != null);
      clientSession.commit();
      System.out.println("consumed all the messages from " + queue);
   }

   private static void sendMessages(final ClientSession clientSession, final SimpleString queue) throws MessagingException
   {
      ClientProducer clientProducer = clientSession.createProducer(queue);
      ClientMessage message = clientSession.createClientMessage(false);
      message.getBody().writeString("Hello, World!");
      clientProducer.send(message);
      clientProducer.send(message);
      clientProducer.send(message);
      clientProducer.send(message);
      System.out.println("sent 4 messages to " + queue);
   }
}
