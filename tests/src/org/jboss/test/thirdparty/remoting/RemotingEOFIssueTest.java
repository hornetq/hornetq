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
package org.jboss.test.thirdparty.remoting;

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
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.Latch;

/**
 * 
 * A RemotingEOFIssueTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class RemotingEOFIssueTest extends MessagingTestCase
{

   public RemotingEOFIssueTest(String name)
   {
      super(name);
   }

   private ConnectionFactory cf;
   
   private Queue queue;
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      ServerManagement.start("all");
      
      ServerManagement.deployQueue("testQueue");
      
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      queue = (Queue)ic.lookup("/queue/testQueue");
      
      this.drainDestination(cf, queue);
      
      ic.close();
      
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      ServerManagement.undeployQueue("testQueue");
      
      ServerManagement.stop();
   }
   
   public void testOutOfOrder() throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection();
         
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         
         Session sess2 = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue);
         
         MessageConsumer cons = sess2.createConsumer(queue);
         
         Latch latch = new Latch();
         
         final int NUM_MESSAGES = 2000;
                  
         MyListener listener = new MyListener(latch, NUM_MESSAGES);
         
         cons.setMessageListener(listener);
         
         conn.start();
         
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("message" + i);
            
            prod.send(tm);
            
            if (i % 10 == 0)
            {
               sess.commit();
            }
         }
         
         latch.acquire();
         
         if (listener.failed)
         {
            fail();
         }
                  
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   class MyListener implements MessageListener
   {
      int c;
      
      Latch latch;
      
      int num;
      
      volatile boolean failed;
      
      MyListener(Latch latch, int num)
      {
         this.latch = latch;
         
         this.num = num;
      }

      public void onMessage(Message msg)
      {
         try
         {
            TextMessage tm = (TextMessage)msg;
            
            log.info("Got message " + tm.getText());
            
            if (!("message" + c).equals(tm.getText()))
            {
               //Failed
               failed = true;

               latch.release();
            }
            
            c++;
            
            if (c == num)
            {
               latch.release();
            }
         }
         catch (JMSException e)
         {
            e.printStackTrace();
            
            //Failed
            failed = true;
            
            latch.release();              
         }
      }
      
   }

}
