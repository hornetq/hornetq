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

import static org.jboss.messaging.core.remoting.TransportType.TCP;

import javax.jms.Session;

import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.api.ClientConnectionFactory;
import org.jboss.jms.client.api.ClientConsumer;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.impl.DestinationImpl;
import org.jboss.messaging.core.impl.MessageImpl;
import org.jboss.messaging.core.remoting.RemotingConfiguration;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class EmbeddedExample
{
   public static void main(String args[]) throws Exception
   {
      RemotingConfiguration remotingConf = new RemotingConfiguration(TCP, "localhost", 5400);
      MessagingServer messagingServer = MessagingServerFactory.createMessagingServer(remotingConf);
      messagingServer.start();
      messagingServer.createQueue("Queue1");
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(remotingConf);
      ClientConnection clientConnection = cf.createConnection(null, null);
      ClientSession clientSession = clientConnection.createClientSession(false, Session.AUTO_ACKNOWLEDGE, false);

      MessageImpl message = new MessageImpl();
      Destination destination = new DestinationImpl(DestinationType.QUEUE, "Queue1", false);
      message.putHeader(org.jboss.messaging.core.Message.TEMP_DEST_HEADER_NAME, destination);
      message.setPayload("hello".getBytes());
      clientSession.send(message);

      ClientConsumer clientConsumer = clientSession.createClientConsumer(destination, null, false, null);
      clientConnection.start();
      Message m = clientConsumer.receive(0);
      System.out.println("m = " + new String(m.getPayload()));
      clientConnection.close();

      MessagingServerFactory.stop(messagingServer);
   }
}
