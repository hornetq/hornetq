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
package org.jboss.jms.example;

import java.util.ArrayList;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.naming.InitialContext;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * A simple JMS example showing the usage of XA support in JMS.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class XAReceiveExample extends JMSExample
{
   private volatile boolean result = true;
   private ArrayList<String> receiveHolder = new ArrayList<String>();
   
   public static void main(String[] args)
   {
      new XAReceiveExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      XAConnection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. Lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         //Step 3. Perform a lookup on the XA Connection Factory
         XAConnectionFactory cf = (XAConnectionFactory) initialContext.lookup("/XAConnectionFactory");

         //Step 4.Create a JMS XAConnection
         connection = cf.createXAConnection();
         
         //Step 5. Start the connection
         connection.start();

         //Step 6. Create a JMS XASession
         XASession xaSession = connection.createXASession();
         
         //Step 7. Create a normal session
         Session normalSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         //Step 8. Create a normal Message Producer
         MessageProducer normalProducer = normalSession.createProducer(queue);

         //Step 9. Get the JMS Session
         Session session = xaSession.getSession();
         
         //Step 10. Create a message consumer
         MessageConsumer xaConsumer = session.createConsumer(queue);
         
         //Step 11. Create two Text Messages
         TextMessage helloMessage = session.createTextMessage("hello");
         TextMessage worldMessage = session.createTextMessage("world");
         
         //Step 12. create a transaction
         Xid xid1 = new XidImpl("xa-example1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
         
         //Step 13. Get the JMS XAResource
         XAResource xaRes = xaSession.getXAResource();
         
         //Step 14. Begin the Transaction work
         xaRes.start(xid1, XAResource.TMNOFLAGS);
         
         //Step 15. Send two messages.
         normalProducer.send(helloMessage);
         normalProducer.send(worldMessage);

         //Step 16. Receive the message
         TextMessage rm1 = (TextMessage)xaConsumer.receive();
         System.out.println("Message received: " + rm1.getText());
         TextMessage rm2 = (TextMessage)xaConsumer.receive();
         System.out.println("Message received: " + rm2.getText());
         
         //Step 17. Stop the work
         xaRes.end(xid1, XAResource.TMSUCCESS);
         
         //Step 18. Prepare
         xaRes.prepare(xid1);
         
         //Step 19. Roll back the transaction
         xaRes.rollback(xid1);
         
         //Step 20. Create another transaction
         Xid xid2 = new XidImpl("xa-example2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
         
         //Step 21. Start the transaction
         xaRes.start(xid2, XAResource.TMNOFLAGS);
         
         //Step 22. receive those messages again
         rm1 = (TextMessage)xaConsumer.receive();
         System.out.println("Message received again: " + rm1.getText());
         rm2 = (TextMessage)xaConsumer.receive();
         System.out.println("Message received again: " + rm2.getText());
         
         //Step 23. Stop the work
         xaRes.end(xid2, XAResource.TMSUCCESS);
         
         //Step 24. Prepare
         xaRes.prepare(xid2);
         
         //Step 25. Commit!
         xaRes.commit(xid2, false);
         
         //Step 26. Check no more messages are received.
         TextMessage rm3 = (TextMessage)xaConsumer.receive(2000);
         if (rm3 == null)
         {
            System.out.println("No message received after commit.");
         }
         else
         {
            result = false;
         }
         
         return result;
      }
      finally
      {
         //Step 27. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }

}
