/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.javaee.example.server;

import javax.annotation.Resource;
import javax.ejb.Remote;
import javax.ejb.Stateless;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.jboss.messaging.jms.JBossQueue;

/**
 * A Stateless Bean that will connect to a remote JBM.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
@Remote(StatelessSenderService.class)
@Stateless
public class StatelessSender implements StatelessSenderService
{

   /**
    *  Resource to be deployed by jms-remote-ds.xml
    *  */
   @Resource(mappedName="java:RemoteJmsXA")
   private ConnectionFactory connectionFactory;
   

   /* (non-Javadoc)
    * @see org.jboss.javaee.example.server.StatelessSenderService#sendHello(java.lang.String)
    */
   public void sendHello(String message) throws Exception
   {
      // Step 5. Define the destination that will receive the message (instead of using JNDI to the remote server)
      JBossQueue destQueue = new JBossQueue("A");
      
      // Step 6. Create a connection to a remote server using a connection-factory (look at the deployed file jms-remote-ds.xml)
      Connection conn = connectionFactory.createConnection("guest", "guest");
      
      // Step 7. Send a message to a QueueA on the remote server, which will be received by MDBQueueA
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(destQueue);
      prod.send(sess.createTextMessage(message));
      
      System.out.println("Step 7 (StatelessSender.java): Sent message \"" + message + "\" to QueueA");

      // Step 8. Close the connection. (Since this is a JCA connection, this will just place the connection back to a connection pool)
      conn.close();
      System.out.println("Step 8 (StatelessSender.java): Closed Connection (sending it back to pool)");
      
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
