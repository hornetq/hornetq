/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import java.sql.PreparedStatement;

import javax.ejb.Remote;
import javax.ejb.Stateless;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.sql.DataSource;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
@Stateless
@Remote(SendMessageService.class)
public class SendMessageBean implements SendMessageService
{
   
   private static final String TABLE = "jbm_example";
   
   public void createTable() throws Exception
   {
      InitialContext ic = new InitialContext();
      DataSource ds = (DataSource)ic.lookup("java:/DefaultDS");
      java.sql.Connection con = ds.getConnection();
      
      // check if the table exists:
      boolean createTable = false;
      try {
         PreparedStatement pr = con.prepareStatement("SELECT * FROM " + TABLE + ";");
         pr.executeQuery();
         pr.close();
      } catch (Exception e)
      {
         createTable = true;
      }
      
      if (createTable)
      {
         PreparedStatement pr = con.prepareStatement("CREATE TABLE " + TABLE + "(id VARCHAR(100), text VARCHAR(100)) TYPE=innodb;");
         pr.execute();
         pr.close();
         System.out.println("Table " + TABLE + " created");
      }
      
      con.close();
   }

   public void sendAndUpdate(String text) throws Exception
   {
      InitialContext ic = null;
      Connection jmsConnection = null;
      java.sql.Connection jdbcConnection = null;

      try
      {
         // Step 1. Lookup the initial context
         ic = new InitialContext();

         // JMS operations
         
         // Step 2. Look up the XA Connection Factory
         ConnectionFactory cf = (ConnectionFactory)ic.lookup("java:/JmsXA");

         // Step 3. Look up the Queue
         Queue queue = (Queue)ic.lookup("queue/testQueue");

         // Step 4. Create a connection, a session and a message producer for the queue
         jmsConnection = cf.createConnection();
         Session session = jmsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = session.createProducer(queue);

         // Step 5. Create a Text Message
         TextMessage message = session.createTextMessage(text);

         // Step 6. Send The Text Message
         messageProducer.send(message);
         System.out.println("Sent message: " + message.getText() + "(" + message.getJMSMessageID() + ")");

         // DB operations
         
         // Step 7. Look up the XA Data Source
         DataSource ds = (DataSource)ic.lookup("java:/XADS");
         
         // Step 8. Retrieve the JDBC connection
         jdbcConnection  = ds.getConnection();
         
         // Step 9. Create the prepared statement to insert the text and the message's ID in the table
         PreparedStatement pr = jdbcConnection.prepareStatement("INSERT INTO " + TABLE + " (id, text) VALUES ('" + message.getJMSMessageID() +
                                                     "', '" +
                                                     text +
                                                     "');");
         
         // Step 10. execute the prepared statement
         pr.execute();
         
         // Step 11. close the prepared statement
         pr.close();
      }
      finally
      {
         // Step 12. Be sure to close all resources!
         if (ic != null)
         {
            ic.close();
         }
         if (jmsConnection != null)
         {
            jmsConnection.close();
         }
         if (jdbcConnection != null)
         {
            jdbcConnection.close();
         }
      }
   }
}
