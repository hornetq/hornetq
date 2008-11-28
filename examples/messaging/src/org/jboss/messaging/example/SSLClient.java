/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.example;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.integration.transports.netty.TransportConstants;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * A simple Client that uses SSL
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SSLClient
{
   public static void main(final String[] args)
   {
      ClientSession clientSession = null;
      try
      {         
         TransportConfiguration tc = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");
         tc.getParams().put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
         ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(tc);         
         clientSession = sessionFactory.createSession(false, true, true);
         SimpleString queue = new SimpleString("queuejms.testQueue");
         ClientProducer clientProducer = clientSession.createProducer(queue);
         ClientMessage message = clientSession.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);
         message.getBody().putString("Hello!");
         clientProducer.send(message);
         ClientConsumer clientConsumer = clientSession.createConsumer(queue);
         clientSession.start();
         ClientMessage msg = clientConsumer.receive(5000);
         msg.acknowledge();
         System.out.println("msg.getPayload() = " + msg.getBody().getString());
      }
      catch(Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (clientSession != null)
         {
            try
            {
               clientSession.close();
            }
            catch (MessagingException e1)
            {
               //
            }
         }
      }
   }
}
