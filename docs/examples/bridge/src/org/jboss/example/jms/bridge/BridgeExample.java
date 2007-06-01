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
package org.jboss.example.jms.bridge;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.example.jms.common.ExampleSupport;

/**
 * This example creates a JMS Connection to a JBoss Messaging instance and then sends a message to a source queue.
 * It then waits to receive the same messages from the target queue.
 * 
 * The example ant script will deploy a message bridge that moves messages from the source to the target queue.
 * 
 * 
 * Since this example is also used by the smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2674 $</tt>
 *
 * $Id: QueueExample.java 2674 2007-05-14 19:57:23Z timfox $
 */
public class BridgeExample extends ExampleSupport
{
   
   public void example() throws Exception
   {
      String source = System.getProperty("example.source.queue");
      
      String target = System.getProperty("example.target.queue");
      
      InitialContext ic = null;
      ConnectionFactory cf = null;
      Connection connection =  null;

      try
      {         
         ic = new InitialContext();
         
         cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
         Queue sourceQueue = (Queue)ic.lookup("/queue/" + source);
         log("Queue " + sourceQueue + " exists");
         
         Queue targetQueue = (Queue)ic.lookup("/queue/" + target);
         log("Queue " + targetQueue + " exists");
         
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer sender = session.createProducer(sourceQueue);
         
         final int NUM_MESSAGES = 10;
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	TextMessage message = session.createTextMessage("Hello!" + i);	
         	
         	sender.send(message);
         	
            log("The message was successfully sent to the " + sourceQueue.getQueueName() + " queue");
         }
         
         MessageConsumer consumer =  session.createConsumer(targetQueue);
         
         connection.start();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
         	TextMessage message = (TextMessage)consumer.receive(10000);
         	
         	assertEquals("Hello!" + i, message.getText());
         	
         	log("The message was received successfully from the " + targetQueue.getQueueName() + " queue");
         }
                  
         displayProviderInfo(connection.getMetaData());                 
      }
      finally
      {         
         if(ic != null)
         {
            try
            {
               ic.close();
            }
            catch(Exception e)
            {
               throw e;
            }
         }
         
         // ALWAYS close your connection in a finally block to avoid leaks.
         // Closing connection also takes care of closing its related objects e.g. sessions.
         closeConnection(connection);
      }
   }
   
   private void closeConnection(Connection con)
   {      
      try
      {
         if (con != null)
         {
            con.close();
         }         
      }
      catch(JMSException jmse)
      {
         log("Could not close connection " + con +" exception was " + jmse);
      }
   }
      
   protected boolean isQueueExample()
   {
      return true;
   }
   
   public static void main(String[] args)
   {
      new BridgeExample().run();
   }
   
}
