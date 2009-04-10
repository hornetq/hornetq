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

         //Step 6. Get the JMS Session
         Session session = xaSession.getSession();
         
         //Step 7. Create two Text Messages
         TextMessage helloMessage = session.createTextMessage("hello");
         TextMessage worldMessage = session.createTextMessage("world");
         
         //Step 8. Create a message producer
         MessageProducer producer = session.createProducer(queue);
         
         //Step 9. Create a message consumer
         MessageConsumer consumer = session.createConsumer(queue);
         consumer.setMessageListener(new SimpleMessageListener());
         
         //Step 10. Create a fake transaction
         Transaction fakeTransaction = new SimpleTransaction();
         
         //Step 11. Create a fake XAResource
         SimpleXAResource xaRes1 = new SimpleXAResource();
         
         //Step 12. Get the JMS XAResource
         XAResource xaRes2 = xaSession.getXAResource();
         
         //Step 13. Enlist the resources
         fakeTransaction.enlistResource(xaRes1);
         fakeTransaction.enlistResource(xaRes2);
         
         //Step 14. Now do the work
         producer.send(helloMessage);
         xaRes1.sentMessage(helloMessage.getText());
         producer.send(worldMessage);
         
         //Step 15. Delist resources
         fakeTransaction.delistResource(xaRes1, XAResource.TMSUCCESS);
         fakeTransaction.delistResource(xaRes2, XAResource.TMSUCCESS);
         
         //Step 16. Now finish the transaction, it will result in rollback!
         try
         {
            fakeTransaction.commit();
            result = false;
         }
         catch (RollbackException e)
         {
            System.out.println("Transaction rolled back, correct!");
         }
         
         //Step 17. Check the result, it should receive none!
         checkNoMessageReceived();

         //Step 17. Now create a new Transaction
         fakeTransaction = new SimpleTransaction();
         
         //Step 18. enlist the resources again
         fakeTransaction.enlistResource(xaRes1);
         fakeTransaction.enlistResource(xaRes2);
         
         //Step 19. do work
         producer.send(helloMessage);
         xaRes1.sentMessage(helloMessage.getText());
         producer.send(worldMessage);
         xaRes1.sentMessage(worldMessage.getText());
         
         //Step 15. Delist resources
         fakeTransaction.delistResource(xaRes1, XAResource.TMSUCCESS);
         fakeTransaction.delistResource(xaRes2, XAResource.TMSUCCESS);
         
         //Step 20. Now commit, should be ok.
         fakeTransaction.commit();
         
         //Step 21. Check the result, all message received
         checkAllMessageReceived();
         
         //Step 22. Now create new transaction, to show XA at the receiving end

         initialContext.close();
         
         return true;
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
         System.out.println("Message received not correct!");
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


   //A simple XAResource used to create different transaction decisions
   public class SimpleXAResource implements XAResource
   {
      String helloWorld = "";
      
      public void sentMessage(String msg)
      {
         helloWorld = helloWorld + " " + msg;
      }

      public void commit(Xid arg0, boolean arg1) throws XAException
      {
      }

      public void end(Xid arg0, int arg1) throws XAException
      {
      }

      public void forget(Xid arg0) throws XAException
      {
      }

      public int getTransactionTimeout() throws XAException
      {
         return 0;
      }

      public boolean isSameRM(XAResource arg0) throws XAException
      {
         return false;
      }

      public int prepare(Xid arg0) throws XAException
      {
         if (helloWorld.equals("hello world"))
         {
            return XA_RDONLY;
         }
         throw new XAException();
      }

      public Xid[] recover(int arg0) throws XAException
      {
         return null;
      }

      public void rollback(Xid arg0) throws XAException
      {
      }

      public boolean setTransactionTimeout(int arg0) throws XAException
      {
         return false;
      }

      public void start(Xid arg0, int arg1) throws XAException
      {
      }
      
   }
   
   public static class SimpleTransaction implements Transaction
   {
      List<XAResource> txResources = new ArrayList<XAResource>();
      List<Xid> xids = new ArrayList<Xid>();
      
//      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

      public void commit() throws RollbackException,
                          HeuristicMixedException,
                          HeuristicRollbackException,
                          SecurityException,
                          SystemException
      {
         boolean ifCommit = true;
         for (int i = 0; i < txResources.size(); i++)
         {
            XAResource res = txResources.get(i);
            Xid xid = xids.get(i);
            try
            {
               int n = res.prepare(xid);
            }
            catch (XAException e)
            {
               ifCommit = false;
            }
         }
        
         if (ifCommit)
         {
            try
            {
               doCommit();
            }
            catch (XAException e)
            {
               throw new HeuristicMixedException();
            }
         }
         else
         {
            try
            {
               doRollback();
               throw new RollbackException();
            }
            catch (XAException e)
            {
               throw new HeuristicRollbackException();
            }
         }
      }
      
      private void doCommit() throws XAException
      {
         for (int i = 0; i < txResources.size(); i++)
         {
            XAResource res = txResources.get(i);
            Xid xid = xids.get(i);
            res.commit(xid, false);
         }
         
      }
      
      private void doRollback() throws XAException
      {
         for (int i = 0; i < txResources.size(); i++)
         {
            XAResource res = txResources.get(i);
            Xid xid = xids.get(i);
            res.rollback(xid);
         }
      }

      public boolean delistResource(XAResource res, int arg1) throws IllegalStateException, SystemException
      {
         boolean result = false;
         for (int i = 0; i < txResources.size(); i++)
         {
            try
            {
               if (txResources.get(i).isSameRM(res)) {
                  XAResource deRes = txResources.remove(i);
                  deRes.end(xids.get(i), XAResource.TMSUCCESS);
                  xids.remove(i);
                  result = true;
                  break;
               }
            }
            catch (XAException e)
            {
            }
         }
         return result;
      }

      public boolean enlistResource(XAResource res) throws RollbackException, IllegalStateException, SystemException
      {
         txResources.add(res);
         Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes()); 
         xids.add(xid);
         try
         {
            res.start(xid, XAResource.TMNOFLAGS);
         }
         catch (XAException e)
         {
            //ignore
         }
         return true;
      }

      public int getStatus() throws SystemException
      {
         return 0;
      }

      public void registerSynchronization(Synchronization arg0) throws RollbackException,
                                                               IllegalStateException,
                                                               SystemException
      {
      }

      public void rollback() throws IllegalStateException, SystemException
      {
         try
         {
            doRollback();
         }
         catch (XAException e)
         {
            throw new SystemException();
         }
      }

      public void setRollbackOnly() throws IllegalStateException, SystemException
      {
      }

   }
   
   public class SimpleMessageListener implements MessageListener
   {

      /* (non-Javadoc)
       * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
       */
      public void onMessage(Message arg0)
      {
         // TODO Auto-generated method stub
         
      }
      
   }

}
