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
package org.jboss.test.messaging.jms.stress;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ServerSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;

import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A Receiver.
 * 
 * Receives messages from a dstination for stress testing
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Receiver extends Runner implements MessageListener
{
   private static final Logger log = Logger.getLogger(Receiver.class);
   
   private static final long RECEIVE_TIMEOUT = 120000;
      
   protected MessageConsumer cons;
   
   protected int count;
   
   protected boolean isListener;
   
   protected Map counts = new HashMap();
   
   protected boolean isCC;
   
   protected Connection conn;
   
   protected ConnectionConsumer cc;
   
   private Object lock1 = new Object();
   
   private Object lock2 = new Object();
   
   private Message theMessage;
   
   private boolean finished;
   
   
   public Receiver(Connection conn, Session sess, int numMessages, Destination dest) throws Exception
   {
      super(sess, numMessages);
      
      this.isListener = true;
      
      this.isCC = true;
      
      sess.setMessageListener(this);
      
      this.cc = conn.createConnectionConsumer(dest, null, new MockServerSessionPool(sess), 10);         
            
   }
   
   public Receiver(Session sess, MessageConsumer cons, int numMessages, boolean isListener) throws Exception
   {
      super(sess, numMessages);
      this.cons = cons;
      this.isListener = isListener;
      if (this.isListener)
      {
         cons.setMessageListener(this);
      }
   }
   
   private boolean done;
   
   public void onMessage(Message m)
   {      
      try
      {            
         synchronized (lock1)
         {
            theMessage = m;
            
            lock1.notify();
         }
         
         //Wait for message to be processed
         synchronized (lock2)
         {
            while (!done && !finished)
            {
               lock2.wait();
            }
            done = false;
         }
         
      }
      catch (Exception e)
      {
         log.error("Failed to put in channel", e);
         setFailed(true);
      }
   }
   
   protected void finished()
   {
      synchronized (lock2)
      {
         finished = true;
         lock2.notify();
      }
   }
   
   
      
   protected Message getMessage() throws Exception
   {
      Message m;
      
      if (isListener)
      {
         synchronized (lock1)
         {     
            long start = System.currentTimeMillis();
            long waitTime = RECEIVE_TIMEOUT;
            while (theMessage == null && waitTime >= 0)
            {
               lock1.wait(waitTime);
               
               waitTime = RECEIVE_TIMEOUT - (System.currentTimeMillis() - start);
            }
            m = theMessage;
            theMessage = null;
         }         
      }
      else
      {         
         m = cons.receive(RECEIVE_TIMEOUT);        
      }
      
      return m;      
   }
   
   protected void processingDone()
   {
      if (isListener)
      {
         synchronized (lock2)
         {
            done = true;
            lock2.notify();
         }
      }
   }
   
   public void run()
   {      
      
      //Small pause so as not to miss any messages in a topic
      try
      {
         Thread.sleep(1000);
      }
      catch (InterruptedException e)
      {         
      }
      
      try
      {             
         String prodName = null;
         Integer msgCount = null;
         
         while (count < numMessages)
         {
            Message m = getMessage();
            
            if (m == null)
            {
               log.error("Message is null");
               setFailed(true);
               processingDone();
               return;
            }
            
            prodName = m.getStringProperty("PROD_NAME");
            msgCount = new Integer(m.getIntProperty("MSG_NUMBER"));
            
            //log.info(this + " Got message " + prodName + ":" + msgCount + "M: " + m.getJMSMessageID());
           
            Integer prevCount = (Integer)counts.get(prodName);
            if (prevCount == null)
            {
               if (msgCount.intValue() != 0)
               {
                  log.error("First message received not zero");
                  setFailed(true);
                  processingDone();
                  return;
               }               
            }
            else
            {
               if (prevCount.intValue() != msgCount.intValue() - 1)
               {
                  log.error("Message out of sequence for " + prodName + ", expected:" + (prevCount.intValue() + 1) + " got " + msgCount);
                  setFailed(true);
                  processingDone();
                  return;
               }
            }
            counts.put(prodName, msgCount);
            
            count++;
            
            processingDone();
         }         
         
         
      }
      catch (Exception e)
      {
         log.error("Failed to receive message", e);
         setFailed(true);
      }
      finally
      {
         if (this.cc != null)
         {
            try
            {
               cc.close();
            }
            catch (JMSException e)
            {
               log.error("Failed to close connection consumer", e);
            }
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

