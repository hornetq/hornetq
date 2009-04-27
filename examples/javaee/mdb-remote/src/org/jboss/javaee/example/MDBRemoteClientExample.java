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
package org.jboss.javaee.example;

import static org.jboss.messaging.integration.transports.netty.TransportConstants.PORT_PROP_NAME;

import java.util.HashMap;
import java.util.Map;

import javax.jms.*;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.naming.InitialContext;

import org.jboss.javaee.example.server.StatelessSenderService;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;

/**
 * 
 * MDB Remote & JCA Configuration Example.
 * 
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class MDBRemoteClientExample
{
   public static void main(String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         //Step 2. Getting a reference to the Stateless Bean
         StatelessSenderService sender = (StatelessSenderService)initialContext.lookup("mdb-example/StatelessSender/remote");
         
         //Step 3. Calling a Stateless Session Bean. You will have more steps on the SessionBean
         sender.sendHello("Hello there MDB!");
         
         System.out.println("Step 3: Invoking the Stateless Bean");
         
         
         Map<String, Object> connectionParams = new HashMap<String, Object>();

         connectionParams.put(PORT_PROP_NAME, 5545);

         TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(),
                                                                                    connectionParams);

         // Step 4. Sending a message to another 
         ConnectionFactory cf = new JBossConnectionFactory(transportConfiguration);
         Queue queue = new JBossQueue("B");
         Connection conn = cf.createConnection("guest", "guest");
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sess.createProducer(queue);
         prod.send(sess.createTextMessage("Hello there MDB talking to another RAR!"));
         conn.close();
         
         
         System.out.println("Step 4: Sent a message to QueueB.");
                  
         
         System.out.println("Follow other steps on the application server's consoles now.");
         
         
         initialContext.close();
      }
      finally
      {
         //Step 11. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }
}
