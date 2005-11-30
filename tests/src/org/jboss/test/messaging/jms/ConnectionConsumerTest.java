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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.Latch;


/**
 * ConnectionConsumer tests
 *
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionConsumerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ConnectionFactory cf;
   protected Destination queue;

   // Constructors --------------------------------------------------

   public ConnectionConsumerTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.init("all");

      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      queue = (Destination)ic.lookup("/queue/Queue");

      log.debug("setup done");
      
      super.drainDestination(cf, queue);
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
      //ServerManagement.deInit();
      super.tearDown();
   }


   // Public --------------------------------------------------------

//   public void testWibble() throws Exception
//   {
//      for (int i = 0; i < 1000; i++)
//      {
//         log.info("run:" + i);
//         dotestCloseWhileProcessing();
//      }
//   }
   
   public void testSimple() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      final int NUM_MESSAGES = 10;
      
      Connection connConsumer = null;
      
      Connection connProducer = null;
      
      try
      {
         connConsumer = cf.createConnection();        
         
         connConsumer.start();
                  
         Session sessCons = connConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         SimpleMessageListener listener = new SimpleMessageListener(NUM_MESSAGES);
         
         sessCons.setMessageListener(listener);
         
         ServerSessionPool pool = new MockServerSessionPool(sessCons);
         
         JBossConnectionConsumer cc = (JBossConnectionConsumer)connConsumer.createConnectionConsumer(queue, null, pool, 1);         
         
         log.info("Started connection consumer");
         
         connProducer = cf.createConnection();
            
         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue);
            
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage m = sessProd.createTextMessage("testing testing");
            prod.send(m);
         }
         
         log.info("Sent messages");
         
         //Wait for messages
         
         listener.waitForLatch(10000);
         
         if (listener.getMsgsReceived() != NUM_MESSAGES)
         {
            fail("Didn't receive all messages");
         }
         
         if (listener.failed)
         {
            fail ("Didn't receive correct messages");
         }
         
         log.info("Received all messages");
         
         log.info("closing connection consumer ...");
         
         cc.close();

         log.info("closing connections ...");

         connProducer.close();
         connProducer = null;
         connConsumer.close();
         connConsumer = null;
         
    
      }
      finally 
      {
         if (connConsumer != null) connConsumer.close();
         if (connConsumer != null) connProducer.close();
      }
   }
  
   
   
   public void testRedeliveryTransacted() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      Connection connConsumer = null;
      
      Connection connProducer = null;
      
      try
      {
         connConsumer = cf.createConnection();        
         
         connConsumer.start();
                  
         Session sessCons = connConsumer.createSession(true, Session.SESSION_TRANSACTED);
         
         RedelMessageListener listener = new RedelMessageListener(sessCons);
         
         sessCons.setMessageListener(listener);
         
         ServerSessionPool pool = new MockServerSessionPool(sessCons);
         
         JBossConnectionConsumer cc = (JBossConnectionConsumer)connConsumer.createConnectionConsumer(queue, null, pool, 1);         
         
         log.info("Started connection consumer");
         
         connProducer = cf.createConnection();
            
         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue);
            
         TextMessage m1 = sessProd.createTextMessage("a");
         TextMessage m2 = sessProd.createTextMessage("b");
         TextMessage m3 = sessProd.createTextMessage("c");
         prod.send(m1);
         prod.send(m2);
         prod.send(m3);
         
         
         log.info("Sent messages");
         
         //Wait for messages
         
         listener.waitForLatch(10000);                  
         
         if (listener.failed)
         {
            fail ("Didn't receive correct messages");
         }
         
         cc.close();
         
         log.info("Closed connection consumer");
         
         connProducer.close();
         connProducer = null;
         connConsumer.close();
         connConsumer = null;
         
    
      }
      finally 
      {
         if (connConsumer != null) connConsumer.close();
         if (connConsumer != null) connProducer.close();
      }
   }
   
   

   public void testCloseWhileProcessing() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      final int NUM_MESSAGES = 10;
      
      Connection connConsumer = null;
      
      Connection connProducer = null;
      
      try
      {
         connConsumer = cf.createConnection();        
         
         connConsumer.start();
                  
         Session sessCons = connConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         SimpleMessageListener listener = new SimpleMessageListener(NUM_MESSAGES);
         
         sessCons.setMessageListener(listener);
         
         ServerSessionPool pool = new MockServerSessionPool(sessCons);
         
         JBossConnectionConsumer cc = (JBossConnectionConsumer)connConsumer.createConnectionConsumer(queue, null, pool, 1);         
         
         log.info("Started connection consumer");
         
         connProducer = cf.createConnection();
            
         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue);
            
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage m = sessProd.createTextMessage("testing testing");
            prod.send(m);
         }
         
         log.info("Sent messages");
         

         cc.close();
         
         connProducer.close();
         connProducer = null;
         connConsumer.close();
         connConsumer = null;
         
         
         
      }
      finally 
      {
         if (connConsumer != null) connConsumer.close();
         if (connConsumer != null) connProducer.close();
      }
   }
   
   
   class SimpleMessageListener implements MessageListener
   {
      Latch latch = new Latch();

      boolean failed;
      
      int msgsReceived;
      
      int numExpectedMsgs;
       
      SimpleMessageListener(int numExpectedMsgs)
      {
         this.numExpectedMsgs = numExpectedMsgs;
         
      }
      
      synchronized void incMsgsReceived()
      {
         msgsReceived++;
         if (msgsReceived == numExpectedMsgs)
         {
            latch.release();
         }
         
      }
      
      synchronized int getMsgsReceived()
      {
         return msgsReceived;
      }
      
      
      void waitForLatch(long timeout) throws Exception
      {
         latch.attempt(timeout);
         //Thread.sleep(2000); //Enough time for postDeliver to complete  
      }
      
      public synchronized void onMessage(Message message)
      {
         try
         {
    
            
            //log.info("Received message " + msgsReceived);
            
            TextMessage tm = (TextMessage)message;
            
            if (!tm.getText().equals("testing testing"))
            {
               failed = true;
            }
            
            incMsgsReceived();
            
         }
         catch (Exception e)
         {
            log.error(e);
            failed = true;
         }
  
      }
      
   }
   
   class RedelMessageListener implements MessageListener
   {
      Latch latch = new Latch();

      boolean failed;
      
      int count;
      
      Session sess;
       
      RedelMessageListener(Session sess)
      {
         this.sess = sess;   
      }
      
      void waitForLatch(long timeout) throws Exception
      {
         latch.attempt(timeout);
         //Thread.sleep(2000);        //Enough time for postdeliver to complete
      }
      
      public synchronized void onMessage(Message message)
      {
         try
         {
            count++;
             
            TextMessage tm = (TextMessage)message;
            
            log.info("Got message " + tm.getText() + " count=" + count);
            
            if (count == 1)
            {
               if (!tm.getText().equals("a"))
               {
                  log.info("Expected a but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
            }
            if (count == 2)
            {
               if (!tm.getText().equals("b"))
               {
                  log.info("Expected b but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
            }
            if (count == 3)
            {
               if (!tm.getText().equals("c"))
               {
                  log.info("Expected c but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
               else
               {
                  if (sess.getAcknowledgeMode() == Session.SESSION_TRANSACTED)
                  {
                     log.info("Rolling back");
                     sess.rollback();
                  }                  
               }
            }
            if (count == 4)
            {
               if (!tm.getText().equals("a"))
               {
                  log.info("Expected a but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
               if (!tm.getJMSRedelivered())
               {
                  log.info("Redelivered flag not set");
                  failed = true;
                  latch.release();
               }
            }
            if (count == 5)
            {
               if (!tm.getText().equals("b"))
               {
                  log.info("Expected b but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
               if (!tm.getJMSRedelivered())
               {
                  log.info("Redelivered flag not set");
                  failed = true;
                  latch.release();
               }
            }
            if (count == 6)
            {
               if (!tm.getText().equals("c"))
               {
                  log.info("Expected c but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
               if (!tm.getJMSRedelivered())
               {
                  log.info("Redelivered flag not set");
                  failed = true;
                  latch.release();
               }
               else
               {
                  if (sess.getAcknowledgeMode() == Session.SESSION_TRANSACTED)
                  {
                     log.info("Committing");
                     sess.commit();
                  }                 
                  latch.release();
               }
            }
            
         }
         catch (JMSException e)
         {
            log.error(e);
            failed = true;
         }
  
      }
      
   }
   
   
   class MockServerSessionPool implements ServerSessionPool
   {
      private ServerSession serverSession;
      
      MockServerSessionPool(Session sess)
      {
         serverSession = new MockServerSession(sess);
      }

      public ServerSession getServerSession() throws JMSException
      {
         return serverSession;
      }      
   }
   
   class MockServerSession implements ServerSession
   {
      Session session;
      
      MockServerSession(Session sess)
      {
         this.session = sess;
      }
      

      public Session getSession() throws JMSException
      {
         return session;
      }

      public void start() throws JMSException
      {
         session.run();
      }
      
   }
   
}
