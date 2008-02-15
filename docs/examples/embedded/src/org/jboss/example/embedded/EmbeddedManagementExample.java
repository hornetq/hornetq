/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
   * by the @authors tag. See the copyright.txt in the distribution for a
   * full listing of individual contributors.
   *
   * This is free software; you can redistribute it and/or modify it
   * under the terms of the GNU Lesser General Public License as
   * published by the Free Software Foundation; either version 2.1 of
   * the License, or (at your option) any later version.
   *
   * This software is distributed in the hope that it will be useful,
   * but WITHOUT ANY WARRANTY; without even the implied warranty of
   * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   * Lesser General Public License for more details.
   *
   * You should have received a copy of the GNU Lesser General Public
   * License along with this software; if not, write to the Free
   * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
   * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
   */
package org.jboss.example.embedded;

import org.jboss.messaging.core.remoting.RemotingConfiguration;
import static org.jboss.messaging.core.remoting.TransportType.TCP;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessagingServerManagement;
import org.jboss.messaging.core.impl.server.MessagingServerImpl;
import org.jboss.messaging.core.impl.server.MessagingServerManagementImpl;
import org.jboss.messaging.core.impl.MessageImpl;
import org.jboss.messaging.core.impl.messagecounter.MessageCounter;
import org.jboss.jms.client.api.*;
import org.jboss.jms.client.impl.ClientConnectionFactoryImpl;
import org.jboss.jms.message.JBossTextMessage;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class EmbeddedManagementExample
{
   public static void main(String args[]) throws Exception
   {
      RemotingConfiguration remotingConf = new RemotingConfiguration(TCP, "localhost", 5400);
      remotingConf.setInvmDisabled(true);
      MessagingServer messagingServer = new MessagingServerImpl(remotingConf);
      messagingServer.start();
      MessagingServerManagementImpl messagingServerManagement = new MessagingServerManagementImpl();
      messagingServerManagement.setMessagingServer(messagingServer);
      messagingServerManagement.start();
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(remotingConf);
      ClientConnection clientConnection = cf.createConnection(null, null);
      ClientSession clientSession = clientConnection.createClientSession(false, true, true, 0);
      String queue = "Queue1";
      clientSession.createQueue(queue, queue, null, false, false);
      ClientProducer clientProducer = clientSession.createProducer();

      clientConnection.start();

      messagingServerManagement.registerMessageCounter(queue);
      Message message = new MessageImpl(JBossTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte) 1);
      messagingServerManagement.startMessageCounter(queue, 0);
      for (int i = 0; i < 1000; i++)
      {
         clientProducer.send(queue, message);
      }

      MessageCounter messageCounter = messagingServerManagement.getMessageCounter(queue);
      System.out.println("messageCounter = " + messageCounter);
      for (int i = 0; i < 2000; i++)
      {
         clientProducer.send(queue, message);
      }

      messageCounter = messagingServerManagement.getMessageCounter(queue);
      System.out.println("messageCounter = " + messageCounter);
      for (int i = 0; i < 3000; i++)
      {
         clientProducer.send(queue, message);
      }

      messageCounter = messagingServerManagement.getMessageCounter(queue);
      System.out.println("messageCounter = " + messageCounter);
      for (int i = 0; i < 4000; i++)
      {
         clientProducer.send(queue, message);
      }

      messageCounter = messagingServerManagement.getMessageCounter(queue);
      System.out.println("messageCounter = " + messageCounter);
      messagingServerManagement.stopMessageCounter(queue);
      messagingServerManagement.startMessageCounter(queue, 5);
      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      Timer timer = new Timer();
      scheduler.schedule(timer, 10, TimeUnit.SECONDS);
      int counter = 0;
      while (timer.isRunning())
      {
         clientProducer.send(queue, message);
         counter++;
      }
      scheduler.shutdown();
      System.out.println("counter = " + counter);
      messageCounter = messagingServerManagement.getMessageCounter(queue);
      System.out.println("messageCounter = " + messageCounter);
      messagingServerManagement.unregisterMessageCounter(queue);
      clientConnection.close();
      messagingServerManagement.stop();
      messagingServer.stop();
   }

   private static class Timer implements Runnable
   {
      boolean running = true;

      public boolean isRunning()
      {
         return running;
      }

      public void run()
      {
         running = false;
      }
   }
}
