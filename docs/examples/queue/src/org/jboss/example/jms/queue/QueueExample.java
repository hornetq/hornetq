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
package org.jboss.example.jms.queue;

import org.jboss.example.jms.common.ExampleSupport;

import javax.naming.InitialContext;
import javax.jms.TextMessage;
import javax.jms.Session;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.MessageProducer;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.JMSException;


/**
 * The example creates a connection to the default provider and uses the connection to send a
 * message to the queue "queue/testQueue". Then, the example creates a second connection to the
 * provider and uses it to receive the message.
 *
 * Since this example is also used by the smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:luc.texier@jboss.org">Luc Texier</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueueExample extends ExampleSupport
{

   public void example() throws Exception
   {
      String destinationName = getDestinationJNDIName();

       InitialContext ic = null;
       ConnectionFactory cf = null;
       Connection connection =  null;
       Connection connection2 =  null;

       try {

           ic = new InitialContext();

           cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
           Queue queue = (Queue)ic.lookup(destinationName);
           log("Queue " + destinationName + " exists");

           connection = cf.createConnection();
           Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           MessageProducer sender = session.createProducer(queue);

           TextMessage message = session.createTextMessage("Hello!");
           sender.send(message);
           log("The message was successfully sent to the " + queue.getQueueName() + " queue");

           connection2 = cf.createConnection();
           Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
           MessageConsumer consumer =  session2.createConsumer(queue);

           connection2.start();

           message = (TextMessage)consumer.receive(2000);
           log("Received message: " + message.getText());
           assertEquals("Hello!", message.getText());

           displayProviderInfo(connection2.getMetaData());


       }finally{

           if(ic != null) {
               try {
                   ic.close();
               }catch(Exception e){
                   throw e;
               }
           }

           //ALWAYS close your connection in a finally block to avoid leaks
           //Closing connection also takes care of closing its related objects e.g. sessions
           closeConnection(connection);

           closeConnection(connection2);

       }

   }

   private void closeConnection(Connection con) throws JMSException {

       try {
		   if (con != null) {
           		con.close();
	       }

       }catch(JMSException jmse) {
           log("Could not close connection " + con +" exception was " +jmse);
           throw jmse;
       }
   }


   protected boolean isQueueExample()
   {
      return true;
   }

   public static void main(String[] args)
   {
      new QueueExample().run();
   }

}
