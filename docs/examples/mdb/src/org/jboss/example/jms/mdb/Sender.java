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
package org.jboss.example.jms.mdb;

import org.jboss.example.jms.common.ExampleSupport;

import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.Queue;
import javax.jms.MessageConsumer;

/**
 * This example deploys a simple Message Driven Bean that processes messages sent to a test queue.
 * Once it receives a message and "processes" it, the MDB sends an acknowledgment message to a
 * temporary destination created by the sender for this purpose. The example is considered
 * successful if the sender receives the acknowledgment message.
 *
 * Since this example is also used by the smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Sender extends ExampleSupport
{
   public void example() throws Exception
   {
      String destinationName = getDestinationJNDIName();



      InitialContext ic = new InitialContext();


      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup(destinationName);



      log("Queue " + destinationName + " exists");



      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer sender = session.createProducer(queue);



      Queue temporaryQueue = session.createTemporaryQueue();
      MessageConsumer consumer = session.createConsumer(temporaryQueue);



      TextMessage message = session.createTextMessage("Hello!");
      message.setJMSReplyTo(temporaryQueue);



      sender.send(message);



      log("The " + message.getText() + " message was successfully sent to the " + queue.getQueueName() + " queue");



      connection.start();



      message = (TextMessage)consumer.receive(2000);



      log("Received message: " + message.getText());



      assertEquals("!olleH", message.getText());



      displayProviderInfo(connection.getMetaData());



      connection.close();
   }




   protected boolean isQueueExample()
   {
      return true;
   }

   public static void main(String[] args)
   {
      new Sender().run();
   }

}
