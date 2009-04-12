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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
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
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * A simple JMS example showing the usage of XA support in JMS.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class XATransactionExample extends JMSExample
{
   private volatile boolean result = true;
   private ArrayList<String> receiveHolder = new ArrayList<String>();
   
   public static void main(String[] args)
   {
      new XATransactionExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      XAConnection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         //Step 3. Perform a lookup on the XA Connection Factory
         XAConnectionFactory cf = (XAConnectionFactory) initialContext.lookup("/XAConnectionFactory");

         //Step 4.Create a JMS XAConnection
         connection = cf.createXAConnection();
         
         //Step 5. Start the connection
         connection.start();

         //Step 5. Create a JMS XASession
         XASession xaSession = connection.createXASession();
         
         //Step 6. Create a normal session
         Session normalSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         //Step 7. Create a normal Message Producer
         MessageProducer normalProducer = normalSession.createProducer(queue);
         
         //Step 8. Create a normal Message Consumer
         MessageConsumer normalConsumer = normalSession.createConsumer(queue);
         normalConsumer.setMessageListener(new SimpleMessageListener());

         //Step 6. Get the JMS Session
         Session session = xaSession.getSession();
         
         //Step 8. Create a message producer
         MessageProducer producer = session.createProducer(queue);
         
         //Step 7. Create two Text Messages
         TextMessage helloMessage = session.createTextMessage("hello");
         TextMessage worldMessage = session.createTextMessage("world");
         
         //Step 10. create a transaction
         Xid xid1 = new XidImpl("xa-example1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
         
         //Step 12. Get the JMS XAResource
         XAResource xaRes = xaSession.getXAResource();
         
         //Step 12. Begin the Transaction work
         xaRes.start(xid1, XAResource.TMNOFLAGS);
         
         //Step 19. do work, sending two messages.
         producer.send(helloMessage);
         producer.send(worldMessage);
         
         Thread.sleep(2000);
         
         //Step 17. Check the result, it should receive none!
         checkNoMessageReceived();
         
         //Step 19. Stop the work
         xaRes.end(xid1, XAResource.TMSUCCESS);
         
         //Step 20. Prepare
         xaRes.prepare(xid1);
         
         //Step 18. Roll back the transaction
         xaRes.rollback(xid1);
         
         //Step. No messages should be received!
         checkNoMessageReceived();
         
         //Step 19. Create another transaction
         Xid xid2 = new XidImpl("xa-example2".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());
         
         //Step 20. Start the transaction
         xaRes.start(xid2, XAResource.TMNOFLAGS);
         
         //Step 21. Re-send those messages
         producer.send(helloMessage);
         producer.send(worldMessage);
         
         //Step 22. Stop the work
         xaRes.end(xid2, XAResource.TMSUCCESS);
         
         //Step 23. Prepare
         xaRes.prepare(xid2);
         
         //Step 23. No messages should be received at this moment
         checkNoMessageReceived();
         
         //Step 23. Commit!
         xaRes.commit(xid2, true);
         
         Thread.sleep(2000);
         
         //Step 21. Check the result, all message received
         checkAllMessageReceived();
         
         return result;
      }
      finally
      {
         //Step 12. Be sure to close our JMS resources!
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
   
   private void checkAllMessageReceived()
   {
      if (receiveHolder.size() != 2)
      {
         System.out.println("Number of messages received not correct ! -- " + receiveHolder.size());
         result = false;
      }
      receiveHolder.clear();
   }

   private void checkNoMessageReceived()
   {
      if (receiveHolder.size() > 0)
      {
         System.out.println("Message received, wrong!");
         result = false;
      }
      receiveHolder.clear();
   }

   
   public class SimpleMessageListener implements MessageListener
   {
      public void onMessage(Message message)
      {
         try
         {
            System.out.println("-----Message received: " + message);
            receiveHolder.add(((TextMessage)message).getText());
         }
         catch (JMSException e)
         {
            result = false;
            e.printStackTrace();
         }
      }
      
   }

}
