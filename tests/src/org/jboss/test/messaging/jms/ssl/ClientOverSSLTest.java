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
package org.jboss.test.messaging.jms.ssl;

import java.io.File;
import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.messaging.core.Configuration;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.util.Logger;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.test.messaging.jms.SerializedClientSupport;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision: 3716 $</tt>
 * 
 */
public class ClientOverSSLTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   public static final String SERIALIZED_CF_FILE_NAME = "ClientOverSSLTest_CFandQueue.ser";
   public static final String MESSAGE_TEXT_FROM_SERVER = "ClientOverSSLTest from server";
   public static final String MESSAGE_TEXT_FROM_CLIENT = "ClientOverSSLTest from client";

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientOverSSLTest.class);

   // Attributes ----------------------------------------------------

   private File serialized;
   private ConnectionFactory cf;
   private Queue queue;

   // Constructors --------------------------------------------------

   public ClientOverSSLTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSSL() throws Exception
   {
      Connection conn = null;

      try
      {
         // spawn a JVM that creates a JMS client, which waits to receive a test
         // message
         Process p = SerializedClientSupport.spawnVM(ClientOverSSL.class
               .getName(), new String[] { serialized.getAbsolutePath() });

         // send the message to the queue
         conn = cf.createConnection();
         conn.start();
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess.createProducer(queue);
         MessageConsumer consumer = sess.createConsumer(queue);

         TextMessage messageFromClient = (TextMessage) consumer.receive(5000);
         assertEquals(MESSAGE_TEXT_FROM_CLIENT, messageFromClient.getText());

         log.info("waiting for the client VM to exit ...");
         p.waitFor();

         assertEquals(0, p.exitValue());

      } finally
      {
         try
         {
            if (conn != null)
               conn.close();
         } catch (Throwable ignored)
         {
            log.warn("Exception ignored:" + ignored.toString(), ignored);
         }
      }
   }

   // Package protected ---------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      System.setProperty(Configuration.REMOTING_ENABLE_SSL_SYSPROP_KEY, "true");

      RemotingService remotingService = servers.get(0).getMessagingServer().getRemotingService();
      remotingService.getRemotingConfiguration().setSSLEnabled(true);
      remotingService.stop();
      remotingService.start();
      
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("ConnectionFactoryOverSSL");
      servers.get(0).deployConnectionFactory("ConnectionFactoryOverSSL", bindings);
      createQueue("QueueOverSSL");

      InitialContext ic = getInitialContext();
      cf = (ConnectionFactory) ic.lookup("/ConnectionFactoryOverSSL");
      queue = (Queue) ic.lookup("/queue/QueueOverSSL");

      serialized = SerializedClientSupport.writeToFile(SERIALIZED_CF_FILE_NAME,
            cf, queue);
   }

   @Override
   protected void tearDown() throws Exception
   {
      removeAllMessages("QueueOverSSL", true);
      servers.get(0).destroyQueue("QueueOverSSL", "/queue/QueueOverSSL");
      servers.get(0).undeployConnectionFactory("ConnectionFactoryOverSSL");
      
 
      if (serialized != null)
      {
         serialized.delete();
      }

      System.setProperty(Configuration.REMOTING_ENABLE_SSL_SYSPROP_KEY, "false");
      RemotingService remotingService = servers.get(0).getMessagingServer().getRemotingService();
      remotingService.getRemotingConfiguration().setSSLEnabled(false);
      remotingService.stop();
      remotingService.start();
        
      super.tearDown();
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
