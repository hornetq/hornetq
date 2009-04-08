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

import javax.ejb.Remote;
import javax.ejb.Stateless;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
@Stateless
@Remote(SendMessageService.class)
public class SendMessageBean implements SendMessageService
{
   public void send() throws Exception
   {
      InitialContext ic = null;
      Connection connection = null;
      try
      {
         System.out.println("SendMessageBean.send");
         //Step 4. Lookup the initial context
         ic = new InitialContext();

         //Step 5. Look Up the XA Connection Factory
         ConnectionFactory cf = (ConnectionFactory) ic.lookup("java:/JmsXA");

         //Step 6. Create a connection
         connection = cf.createConnection();

         //Step 7. Lookup the Queue
         Queue queue = (Queue) ic.lookup("queue/testQueue");

         //Step 8. Create a session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 9. Create a Message Producer
         MessageProducer messageProducer = session.createProducer(queue);

         //Step 10. Create a Text Message
         TextMessage message = session.createTextMessage("this is a reply!");

         //Step 11. Send The Text Message
         messageProducer.send(message);

         //todo something else in the same tx, update database
      }
      finally
      {
         //Step 12. Be sure to close our JMS resources!
         if(ic != null)
         {
            ic.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }
}
