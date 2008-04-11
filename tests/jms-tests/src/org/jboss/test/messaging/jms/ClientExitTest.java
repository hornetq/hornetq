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

import java.io.File;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.test.messaging.JBMServerTestCase;

/**
 * A test that makes sure that a Messaging client gracefully exists after the last connection is
 * closed. Test for http://jira.jboss.org/jira/browse/JBMESSAGING-417.
 *
 * This is not technically a crash test, but it uses the same type of topology as the crash tests
 * (local server, remote VM client).
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version 1.1
 *
 * $Id$
 */
public class ClientExitTest extends JBMServerTestCase
{
   // Constants ------------------------------------------------------------------------------------

   public static final String SERIALIZED_CF_FILE_NAME = "CFandQueue.ser";
   public static final String MESSAGE_TEXT = "kolowalu";

   // Static ---------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientExitTest.class);

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClientExitTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testGracefulClientExit() throws Exception
   {
      File serialized = null;
      
      Connection conn = null;

      try
      {

         //localServer.start("all", true);
         createQueue("Queue");

         // lookup the connection factory and the queue which we'll send to the client VM via a
         // serialized instances saved in file

         InitialContext ic = getInitialContext();
         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
         Queue queue = (Queue)ic.lookup("/queue/Queue");

         serialized = SerializedClientSupport.writeToFile(SERIALIZED_CF_FILE_NAME, cf, queue);

         // spawn a JVM that creates a JMS client, which sends a test message
         Process p = SerializedClientSupport.spawnVM(GracefulClient.class.getName(), new String[] {serialized.getAbsolutePath()});

         // read the message from the queue

         conn = cf.createConnection();
         conn.start();
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(queue);
         TextMessage tm = (TextMessage)cons.receive(15000);

         assertNotNull(tm);
         assertEquals(MESSAGE_TEXT, tm.getText());

         // the client VM should exit by itself. If it doesn't, that means we have a problem
         // and the test will timeout
         log.info("waiting for the client VM to exit ...");
         p.waitFor();

         assertEquals(0, p.exitValue());
      }
      finally
      {
         try
         {
            if (conn != null)
               conn.close();

            // TODO delete the file
            if (serialized != null)
            {
               serialized.delete();
            }

         }
         catch (Throwable ignored)
         {
            log.warn("Exception ignored:" + ignored.toString(), ignored);
         }
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
