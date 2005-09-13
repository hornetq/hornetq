/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
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
import org.jboss.jms.delegate.ConnectionDelegate;
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

   protected InitialContext initialContext;
   
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
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      queue = (Destination)initialContext.lookup("/queue/Queue");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.deInit();
      super.tearDown();
   }


   // Public --------------------------------------------------------
   
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
         
         log.trace("Started connection consumer");
         
         connProducer = cf.createConnection();
            
         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue);
            
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage m = sessProd.createTextMessage("testing testing");
            prod.send(m);
         }
         
         log.trace("Sent messages");
         
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
         
         log.trace("Received all messages");
         
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
         
         log.trace("Started connection consumer");
         
         connProducer = cf.createConnection();
            
         Session sessProd = connProducer.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sessProd.createProducer(queue);
            
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage m = sessProd.createTextMessage("testing testing");
            prod.send(m);
         }
         
         log.trace("Sent messages");
         

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
      }
      
      public void onMessage(Message message)
      {
         try
         {
            
            log.trace("Received message");
            
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
