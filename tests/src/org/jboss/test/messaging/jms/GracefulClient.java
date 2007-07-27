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
package org.jboss.test.messaging.jms;

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
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class GracefulClient
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   public static void main(String[] args) throws Exception
   {

      String serializedFileName = args[0];

      // we don't want to mess with JNDI, read the connection factory and the queue from their
      // serialized format, from disk
      ObjectInputStream ois =
         new ObjectInputStream(new FileInputStream(serializedFileName));
      ConnectionFactory cf =(ConnectionFactory)ois.readObject();
      Queue queue = (Queue)ois.readObject();

      ois.close();

      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(queue);
      MessageConsumer cons = sess.createConsumer(queue);

      prod.send(sess.createTextMessage(ClientExitTest.MESSAGE_TEXT));

      conn.start();

      // block in receiving for 5 secs, we won't receive anything

      cons.receive(5000);

      // this should silence any non-daemon thread and allow for graceful exit
      conn.close();

//      new Thread(new Runnable()
//      {
//         public void run()
//         {
//            // spin around in circles
//            while(true)
//            {
//               try
//               {
//                  Thread.sleep(1000);
//               }
//               catch(Exception e)
//               {
//                  // OK
//               }
//            }
//         }
//      }, "Spinner").start();
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
