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
package org.jboss.core.example;

import org.jboss.common.example.JBMExample;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;

/**
 * 
 * A PerfExample
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class PerfExample extends JBMExample
{
   public static void main(String[] args)
   {
      new PerfExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      final String perfAddress = "perfAddress";
      
      final String perfQueueName = "perfQueue";
      
      ClientSessionFactory factory = null;

      ClientSession session = null;

      try
      {
         factory = new ClientSessionFactoryImpl(new TransportConfiguration(NettyConnectorFactory.class.getName()));

         session = factory.createSession();
         
         session.createQueue(perfAddress, perfQueueName, true);

         ClientProducer producer = session.createProducer(perfAddress);
         
         ClientMessage message = session.createClientMessage(true);

         message.getBody().writeString("Hello");

         producer.send(message);

         session.start();

         ClientConsumer consumer = session.createConsumer(perfQueueName);

         ClientMessage msgReceived = consumer.receive();
         
         System.out.println("message = " + msgReceived.getBody().readString());
         
         consumer.close();
         
         session.deleteQueue(perfQueueName);
                  
         return true;
      }
      finally
      {
         if (session != null)
         {           
            session.close();
         }

         if (factory != null)
         {
            factory.close();
         }
      }
   }

}
