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
package org.jboss.example.jms.statelessclustered.client;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.TextMessage;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import javax.naming.Context;
import javax.naming.InitialContext;

import org.jboss.example.jms.common.ExampleSupport;
import org.jboss.example.jms.statelessclustered.bean.StatelessClusteredSessionExample;
import org.jboss.example.jms.statelessclustered.bean.StatelessClusteredSessionExampleHome;

/**
 * This example deploys a simple clustered Stateless Session Bean that is used as a proxy to send
 * and receive JMS messages in a managed environment.
 *
 * Since this example is also used by the smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1843 $</tt>
 *
 * $Id: Client.java 1843 2006-12-21 23:41:19Z timfox $
 */
public class Client extends ExampleSupport
{
   private InitialContext createHAInitialContext() throws Exception
   {
      Properties p = new Properties();
      p.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
      p.put(Context.URL_PKG_PREFIXES, "jboss.naming:org.jnp.interfaces");
      p.put(Context.PROVIDER_URL, "localhost:1100,localhost:1200");
      return new InitialContext(p);
   }

   public void example() throws Exception
   {            
      InitialContext haic = createHAInitialContext();
      
      StatelessClusteredSessionExampleHome home =
         (StatelessClusteredSessionExampleHome)haic.lookup("ejb/StatelessClusteredSessionExample");            
      
      StatelessClusteredSessionExample bean = home.create();

      String queueName = getDestinationJNDIName();
      String text = "Hello!";

      bean.drain(queueName);

      bean.send("Hello!", queueName);
      log("The " + text + " message was successfully sent to the " + queueName + " queue");

      int num = bean.browse(queueName);
      bean.remove();

      assertEquals(1, num);

      log("Queue browse result: " + num);

      Queue queue = (Queue)haic.lookup(queueName);
      ConnectionFactory cf = (ConnectionFactory) new InitialContext().lookup("/XAConnectionFactory");
      Connection conn = cf.createConnection();

      try
      {
         conn.start();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);         
         MessageConsumer consumer = session.createConsumer(queue);
         
         System.out.println("blocking to receive message from queue " + queueName + " ...");
         TextMessage tm = (TextMessage)consumer.receive(5000);
         
         if (tm == null)
         {
            throw new Exception("No message!");
         }
         
         System.out.println("Message " + tm.getText() + " received");
         
         assertEquals("Hello!", tm.getText());
      }
      finally
      {
         conn.close();
      }

   }
   
   protected boolean isQueueExample()
   {
      return true;
   }
   
   public static void main(String[] args)
   {
      new Client().run();
   }   
}
