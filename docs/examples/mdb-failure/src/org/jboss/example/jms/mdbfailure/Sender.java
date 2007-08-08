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
package org.jboss.example.jms.mdbfailure;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.example.jms.common.ExampleSupport;

/**
 * This example deploys a simple Message Driven Bean that processes messages sent to a test queue.
 * The MDB container is configured to use CMT.
 *
 * We send a special tagged message (we do this by adding a custom property to the message) to the
 * MDB. The MDB is programmed to "fail" when handling such a message, by throwing a
 * RuntimeException, but not before "clearing" the failure-inducing tag.
 *
 * The expected behavior for the JMS provider is to redeliver the message, this time without the
 * "failure tag". The MDB is supposed to process it and put it on response queue, from were it will
 * be read by the client.
 *
 * The example is considered successful if the client receives the "processed" message.
 *
 * Since this example is also used by the smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 884 $</tt>
 *
 * $Id: Sender.java 884 2006-04-12 01:04:10Z ovidiu $
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



      log("The \"" + message.getText() + "\" message was successfully sent to the " + queue.getQueueName() + " queue");



      connection.start();



      message = (TextMessage)consumer.receive(5000);


      if (message == null)
      {
         throw new Exception("Have not received any reply. The example failed!");
      }


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
