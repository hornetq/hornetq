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
package org.jboss.test.messaging.jms.crash;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;


/**
 * Code to be run in an external VM, via main().
 * 
 * This client will open a connection, receive a message and crash.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class CrashClient
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   public static void main(String[] args) throws Exception
   {

      String serializedFileName = args[0];
      int numberOfConnections = Integer.parseInt(args[1]);

      // we don't want to mess with JNDI, read the connection factory and the queue from their
      // serialized format, from disk
      ObjectInputStream ois =
         new ObjectInputStream(new FileInputStream(serializedFileName));
      ConnectionFactory cf =(ConnectionFactory)ois.readObject();
      Queue queue = (Queue)ois.readObject();

      ois.close();
      
      // create one connection which is used
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = sess.createConsumer(queue);
      MessageProducer prod = sess.createProducer(queue);

      if (numberOfConnections > 1)
      {
         // create (num - 1) unused connections
         for (int i = 0; i < numberOfConnections - 1; i++)
         {
            cf.createConnection();         
         }
      }
      
      prod.send(sess.createTextMessage(ClientCrashTest.MESSAGE_TEXT_FROM_CLIENT));

      
      conn.start();
      cons.receive(5000);
      
      // crash
      System.exit(9);
   }

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Command implementation -----------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
