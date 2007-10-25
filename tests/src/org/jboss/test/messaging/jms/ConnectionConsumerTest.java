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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.jms.client.JBossConnectionConsumer;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.Latch;


/**
 * ConnectionConsumer tests
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionConsumerTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionConsumerTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------   

   // Public --------------------------------------------------------

   public void testSimple() throws Exception
   {
      if (ServerManagement.isRemote()) return;

      final int NUM_MESSAGES = 100;

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

         JBossConnectionConsumer cc = (JBossConnectionConsumer)connConsumer.createConnectionConsumer(queue1, null, pool, 1);

         connProducer = cf.createConnection();

         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue1);

         forceGC();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage m = sessProd.createTextMessage("testing testing");
            prod.send(m);
         }

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

         cc.close();

         connProducer.close();
         connProducer = null;
         connConsumer.close();
         connConsumer = null;
      }
      finally
      {
         if (connConsumer != null) connConsumer.close();
         if (connProducer != null) connProducer.close();
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
         
         JBossConnectionConsumer cc = (JBossConnectionConsumer)connConsumer.createConnectionConsumer(queue1, null, pool, 1);         
         
         connProducer = cf.createConnection();
            
         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue1);
            
         TextMessage m1 = sessProd.createTextMessage("a");
         TextMessage m2 = sessProd.createTextMessage("b");
         TextMessage m3 = sessProd.createTextMessage("c");
         prod.send(m1);
         prod.send(m2);
         prod.send(m3);
            
         //Wait for messages
         
         listener.waitForLatch(10000);                  
         
         if (listener.failed)
         {
            fail ("Didn't receive correct messages");
         }
         
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
      
   
   public void testRedeliveryTransactedDifferentConnection() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      Connection connConnectionConsumer = null;
      
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
         
         connConnectionConsumer = cf.createConnection();
         
         connConnectionConsumer.start();
         
         JBossConnectionConsumer cc = (JBossConnectionConsumer)connConnectionConsumer.createConnectionConsumer(queue1, null, pool, 1);         
         
         connProducer = cf.createConnection();
            
         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue1);
            
         TextMessage m1 = sessProd.createTextMessage("a");
         TextMessage m2 = sessProd.createTextMessage("b");
         TextMessage m3 = sessProd.createTextMessage("c");
         prod.send(m1);
         prod.send(m2);
         prod.send(m3);
          
         //Wait for messages
         
         listener.waitForLatch(10000);                  
         
         if (listener.failed)
         {
            fail ("Didn't receive correct messages");
         }
         
         cc.close();
           
         connProducer.close();
         connProducer = null;
         connConsumer.close();
         connConsumer = null;
         connConnectionConsumer.close();
         connConnectionConsumer = null;    
      }
      finally 
      {
         if (connConsumer != null) connConsumer.close();
         if (connConsumer != null) connProducer.close();
         if (connConnectionConsumer != null) connConnectionConsumer.close();
      }
   }

   public void testCloseWhileProcessing() throws Exception
   {
      if (ServerManagement.isRemote()) return;

      final int NUM_MESSAGES = 100;

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

         JBossConnectionConsumer cc = (JBossConnectionConsumer)connConsumer.createConnectionConsumer(queue1, null, pool, 1);

         connProducer = cf.createConnection();

         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue1);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage m = sessProd.createTextMessage("testing testing");
            prod.send(m);
         }

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

         removeAllMessages(queue1.getQueueName(), true, 0);
      }
   }

//   Commented out until http://jira.jboss.com/jira/browse/JBMESSAGING-1099 is complete   
//   public void testStopWhileProcessing() throws Exception
//   {
//      if (ServerManagement.isRemote()) return;
//
//
//      Connection connConsumer = null;
//
//      try
//      {
//         connConsumer = cf.createConnection();
//
//         connConsumer.start();
//
//         Session sessCons = connConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         SimpleMessageListener listener = new SimpleMessageListener(0);
//
//         sessCons.setMessageListener(listener);
//
//         ServerSessionPool pool = new MockServerSessionPool(sessCons);
//
//         JBossConnectionConsumer cc = (JBossConnectionConsumer)connConsumer.createConnectionConsumer(queue1, null, pool, 1);
//
//         ServerManagement.stop();
//         connConsumer.close();
//         connConsumer = null;
//      }
//      finally
//      {
//         if (connConsumer != null) connConsumer.close();
//      }
//   }


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
      }
      
      public synchronized void onMessage(Message message)
      {
         try
         {
            count++;
             
            TextMessage tm = (TextMessage)message;
            
            log.trace("Got message " + tm.getText() + " count=" + count);
            
            if (count == 1)
            {
               log.trace("delivery count:" + tm.getIntProperty("JMSXDeliveryCount"));
               
               if (!tm.getText().equals("a"))
               {
                  log.trace("Expected a but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
            }
            if (count == 2)
            {
               log.trace("delivery count:" + tm.getIntProperty("JMSXDeliveryCount"));
               
               if (!tm.getText().equals("b"))
               {
                  log.trace("Expected b but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
            }
            if (count == 3)
            {
               log.trace("delivery count:" + tm.getIntProperty("JMSXDeliveryCount"));
               
               if (!tm.getText().equals("c"))
               {
                  log.trace("Expected c but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
               else
               {
                  if (sess.getAcknowledgeMode() == Session.SESSION_TRANSACTED)
                  {
                     log.trace("Rolling back");
                     sess.rollback();
                  }                  
               }
            }
            if (count == 4)
            {
               log.trace("delivery count:" + tm.getIntProperty("JMSXDeliveryCount"));
               
               if (!tm.getText().equals("a"))
               {
                  log.trace("Expected a but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
               if (!tm.getJMSRedelivered())
               {

                  failed = true;
                  latch.release();
               }
            }
            if (count == 5)
            {
               log.trace("delivery count:" + tm.getIntProperty("JMSXDeliveryCount"));
               
               if (!tm.getText().equals("b"))
               {
                  log.trace("Expected b but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
               if (!tm.getJMSRedelivered())
               {
                  log.trace("Redelivered flag not set");
                  failed = true;
                  latch.release();
               }
            }
            if (count == 6)
            {
               log.trace("delivery count:" + tm.getIntProperty("JMSXDeliveryCount"));
               
               if (!tm.getText().equals("c"))
               {
                  log.trace("Expected c but was " + tm.getText());
                  failed = true;
                  latch.release();
               }
               if (!tm.getJMSRedelivered())
               {
                  log.trace("Redelivered flag not set");
                  failed = true;
                  latch.release();
               }
               else
               {
                  if (sess.getAcknowledgeMode() == Session.SESSION_TRANSACTED)
                  {
                     log.trace("Committing");
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
