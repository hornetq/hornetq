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
package org.jboss.test.messaging.jms.clustering;

import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.tools.ServerManagement;

import javax.jms.*;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * A test where we kill multiple nodes and make sure the failover works correctly in these condtions
 * too.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MultipleFailoverTest extends ClusteringTestBase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public MultipleFailoverTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testAllKindsOfServerFailures() throws Exception
   {
      Connection conn = null;
      TextMessage m = null;
      MessageProducer prod = null;
      MessageConsumer cons = null;

      try
      {
         conn = this.createConnectionOnServer(cf, 1);
         conn.start();

         // send/receive message
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         prod = s.createProducer(queue[0]);
         cons = s.createConsumer(queue[0]);
         prod.send(s.createTextMessage("step1"));
         m = (TextMessage)cons.receive();
         assertNotNull(m);
         assertEquals("step1", m.getText());

         log.info("killing node 1 ....");

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // send/receive message
         prod.send(s.createTextMessage("step2"));
         m = (TextMessage)cons.receive();
         assertNotNull(m);
         assertEquals("step2", m.getText());

         log.info("########");
         log.info("######## STARTING NODE 3");
         log.info("########");

         ServerManagement.start(3, "all", false);
         deployQueue("testDistributedQueue", 3);
         deployTopic("testDistributedTopic", 3);

         // send/receive message
         prod.send(s.createTextMessage("step3"));
         m = (TextMessage)cons.receive();
         assertNotNull(m);
         assertEquals("step3", m.getText());

         log.info("killing node 2 ....");

         ServerManagement.kill(2);

         log.info("########");
         log.info("######## KILLED NODE 2");
         log.info("########");

         // send/receive message
         prod.send(s.createTextMessage("step4"));
         m = (TextMessage)cons.receive();
         assertNotNull(m);
         assertEquals("step4", m.getText());

         log.info("########");
         log.info("######## STARTING NODE 4");
         log.info("########");

         ServerManagement.start(4, "all", false);
         log.info("deploying queue on4");
         deployQueue("testDistributedQueue", 4);
         deployTopic("testDistributedTopic", 4);
         log.info("deployed it");

         // send/receive message
         prod.send(s.createTextMessage("step5"));
         m = (TextMessage)cons.receive();
         assertNotNull(m);
         assertEquals("step5", m.getText());

         log.info("killing node 3 ....");

         ServerManagement.kill(3);

         log.info("########");
         log.info("######## KILLED NODE 3");
         log.info("########");

         // send/receive message
         prod.send(s.createTextMessage("step6"));
         m = (TextMessage)cons.receive();
         assertNotNull(m);
         assertEquals("step6", m.getText());

         log.info("########");
         log.info("######## STARTING NODE 1");
         log.info("########");

         ServerManagement.start(1, "all", false);
         deployQueue("testDistributedQueue", 1);
         deployTopic("testDistributedTopic", 1);

         // send/receive message
         prod.send(s.createTextMessage("step7"));
         m = (TextMessage)cons.receive();
         assertNotNull(m);
         assertEquals("step7", m.getText());

         log.info("killing node 4 ....");

         ServerManagement.kill(4);

         log.info("########");
         log.info("######## KILLED NODE 4");
         log.info("########");

         // send/receive message
         prod.send(s.createTextMessage("step8"));
         m = (TextMessage)cons.receive();
         assertNotNull(m);
         assertEquals("step8", m.getText());
         
         log.info("Got to the end");

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   
   
   public void testFailoverFloodTwoServers() throws Exception
   {
      JBossConnection conn = null;

      try
      {
         conn = (JBossConnection)this.createConnectionOnServer(cf, 1);

         Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session sessCons = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sessCons.createConsumer(queue[1]);

         MyListener list = new MyListener();

         cons.setMessageListener(list);

         conn.start();

         MessageProducer prod = sessSend.createProducer(queue[1]);

         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         int count = 0;
         
         Killer killer = new Killer();
         
         Thread t = new Thread(killer);
         
         t.start();
         
         while (!killer.isDone())
         {
            TextMessage tm = sessSend.createTextMessage("message " + count);
            tm.setIntProperty("cnt", count);

            prod.send(tm);
            
            if (count % 100 == 0)
            {
               log.info("sent " + count + " server id " + conn.getServerID());
            }
            
            count++;
            
            Thread.sleep(5);
         }
         
         log.info("done send");
         
         log.info("Waiting to join thread");
         t.join(5 * 60 * 60 * 1000);
         log.info("joined");
         
         if (killer.failed)
         {
            fail();
         }
         
         //We check that we received all the message
         //we allow for duplicates, see http://jira.jboss.org/jira/browse/JBMESSAGING-604
         //We are using auto_ack with a listener so duplicates are ok
         
         if (!list.waitFor(count - 1))
         {
         	fail("Timed out waiting for message");
         }
         
         conn.close();
         conn = null;
         
                  
         count = 0;
         Iterator iter = list.msgs.iterator();
         while (iter.hasNext())
         {
            Integer i = (Integer)iter.next();
            
            if (i.intValue() != count)
            {
               fail("Missing message " + i);
            }
            count++;
         }
         
         if (list.failed)
         {
            fail();
         }
      }
      catch (Exception e)
      {
         log.error("Failed", e);
         throw e;
      }
      finally
      {         
         if (conn != null)
         {
            log.info("closing connection");
            try
            {
               conn.close();
            }
            catch (Exception ignore)
            {
            }
            log.info("closed connection");
         }
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 3;

      super.setUp();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   class Killer implements Runnable
   { 
      volatile boolean failed;
      
      volatile boolean done;
      
      public boolean isDone()
      {
         return done;
      }
      
      public void run()
      {
         try
         {                                     
            Thread.sleep(5000);
               
            log.info("Killing server 1");
            ServerManagement.kill(1);
            log.info("Killed server 1");
            
            Thread.sleep(5000);
            
            log.info("starting server 1");
            ServerManagement.start(1, "all", false);
            log.info("server 1 started");
            log.info("*** TRYING TO DEPLOY QUEUE");
            deployQueue("testDistributedQueue", 1);
            log.info("DEPLOYED QUEUE");
            deployTopic("testDistributedTopic", 1);
            log.info("Deployed destinations");
            
            Thread.sleep(5000);
            
            log.info("Killing server 2");
            ServerManagement.kill(2);
            
            Thread.sleep(5000);
            
            log.info("Starting server 2");
            ServerManagement.start(2, "all", false);
            log.info("server 2 started");
            deployQueue("testDistributedQueue", 2);
            deployTopic("testDistributedTopic", 2);
            log.info("Deployed destinations");            
            
            Thread.sleep(5000);
            
            log.info("Killing server 1");
            ServerManagement.kill(1);
            
            Thread.sleep(5000);
            
            log.info("Starting server 1");
            ServerManagement.start(1, "all", false);
            log.info("server 1 started");
            deployQueue("testDistributedQueue", 1);
            deployTopic("testDistributedTopic", 1);
            log.info("Deployed destinations");            
            
            
            Thread.sleep(5000);
            
            log.info("Killing server 2");
            ServerManagement.kill(2);
            
            Thread.sleep(5000);
            
            log.info("Starting server 2");
            ServerManagement.start(2, "all", false);
            deployQueue("testDistributedQueue", 2);
            deployTopic("testDistributedTopic", 2);
            
            Thread.sleep(5000);
            
            log.info("Killing server 1");
            ServerManagement.kill(1);
            
            Thread.sleep(5000);
            
            log.info("Starting server 1");
            ServerManagement.start(1, "all", false);
            deployQueue("testDistributedQueue", 1);
            deployTopic("testDistributedTopic", 1);
            
            Thread.sleep(10000);
            
            log.info("killer DONE");
         }
         catch (Exception e)
         {               
         	log.error("Killer failed", e);
            failed = true;
         }
         
         done = true;
      }
      
   }
   
   class MyListener implements MessageListener
   {
      volatile boolean failed;
      
      Set msgs = new TreeSet();
      
      int maxcnt = 0;
      
      private Object obj = new Object();
      
      boolean waitFor(int i)
      {
      	log.info("Waiting for message " + i);
      	synchronized (obj)
      	{
      		log.info("here");
      		long toWait = 30000;
      		while (maxcnt < i && toWait > 0)
      		{
      			long start = System.currentTimeMillis();
      			try
      			{      		
      				obj.wait(60000);
      			}
      			catch (InterruptedException e)
      			{}
      			if (maxcnt < i)
      			{
      				toWait -= System.currentTimeMillis() - start;
      			}            			      
      		}
      		return maxcnt == i;
      	}
      	
      }
   
      public void onMessage(Message msg)
      {
         try
         {
            int cnt = msg.getIntProperty("cnt");
                        
            if (cnt % 100 == 0)
            {
               log.info(this + " Received message " + cnt);
            }
            
            /*
            
            IMPORTANT NOTE 
            
            http://jira.jboss.org/jira/browse/JBMESSAGING-604
             
            There will always be the possibility that duplicate messages can be received until
            we implement duplicate message detection.
            Consider the following possibility:
            A message is sent the send succeeds on the server
            The message is delivered to the client and acked.
            The ack removes it from the server
            The server then fails *before* the original send message has written its response
            to the socket
            The client receives a socket exception
            Failover kicks in
            After failover the client resumes the send
            The message gets delivered again
            And yes, this was actually seen to happen in the logs :)
            
            Therefore we only count that the total messages were received
            */      
              
            msgs.add(new Integer(cnt));
            
            maxcnt = Math.max(maxcnt, cnt);
            
            synchronized (obj)
            {
            	obj.notify();
            }         
         }
         catch (Exception e)
         {
            log.error("Failed to receive", e);
            failed = true;
         }
      }
      
   }
}
