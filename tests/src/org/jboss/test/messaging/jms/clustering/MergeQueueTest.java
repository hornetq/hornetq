/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 */
public class MergeQueueTest extends ClusteringTestBase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public MergeQueueTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testMergeQueue() throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;

      try
      {
         // Objects Server0
         conn0 = createConnectionOnServer(cf, 0);

         assertEquals(0, getServerId(conn0));

         Session session0 = conn0.createSession(true, Session.SESSION_TRANSACTED);

         conn0.start();

         MessageProducer producer0 = session0.createProducer(queue[0]);

         producer0.setDeliveryMode(DeliveryMode.PERSISTENT);

         MessageConsumer consumer0 = session0.createConsumer(queue[0]);

         for (int i = 0; i < 10; i++)
         {
            producer0.send(session0.createTextMessage("message " + i));
         }

         session0.commit();

         TextMessage msg;

         for (int i = 0; i < 5; i++)
         {
            msg = (TextMessage)consumer0.receive(5000);
            assertNotNull(msg);
            log.info("msg = " + msg.getText());
            assertEquals("message " + i, msg.getText());
         }

         session0.commit();
         log.info("****Closing consumer");
         consumer0.close();


         // Objects Server1
         conn1 = createConnectionOnServer(cf, 1);

         assertEquals(1, getServerId(conn1));

         conn1.start();

         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer1 = session1.createProducer(queue[1]);

         producer1.setDeliveryMode(DeliveryMode.PERSISTENT);

         for (int i = 10; i < 20; i++)
         {
            producer1.send(session0.createTextMessage("message " + i));
         }

         //At this point there should be 5 messages on the node 0 queue (5-9)
         //and 10 messages on the node 1 queue (10-19)
         
         ServerManagement.kill(1);

         consumer0 = session0.createConsumer(queue[0]);

         Set ids = new HashSet();
         for (int i = 5; i < 20; i++)
         {
            msg = (TextMessage)consumer0.receive(5000);
            assertNotNull(msg);
            log.info("msg = " + msg.getText());
            ids.add(msg.getText());
         }
         
         for (int i = 5; i < 20; i++)
         {
         	assertTrue(ids.contains("message " + i));
         }

         assertNull(consumer0.receive(5000));

         session0.commit();
      }
      finally
      {
         if (conn0!=null)
         {
            conn0.close();
         }

         if (conn1!=null)
         {
            conn1.close();
         }
      }
   }

   public void testMergeQueue2() throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;

      try
      {
      	//node 0
         conn0 = createConnectionOnServer(cf, 0);

         assertEquals(0, getServerId(conn0));

         Session session0 = conn0.createSession(true, Session.SESSION_TRANSACTED);

         conn0.start();

         MessageProducer producer0 = session0.createProducer(queue[0]);

         producer0.setDeliveryMode(DeliveryMode.PERSISTENT);

         MessageConsumer consumer0 = session0.createConsumer(queue[0]);

         //Send messages 0 - 9 on node 0
         for (int i= 0; i < 10; i++)
         {
            producer0.send(session0.createTextMessage("message " + i));
         }

         session0.commit();

         TextMessage msg;

         
         //Consume messages 0 - 4 on node node 0 transactionally
         for (int i = 0; i < 5; i++)
         {
            msg = (TextMessage)consumer0.receive(5000);
            assertNotNull(msg);
            log.info("msg = " + msg.getText());
            assertEquals("message " + i, msg.getText());
         }

         //commit the session
         session0.commit();
         consumer0.close();

         log.info("Consumed messages 0 - 4 on node 0");
         
         // node 1
         conn1 = createConnectionOnServer(cf, 1);

         assertEquals(1, getServerId(conn1));

         conn1.start();

         Session session1 = conn1.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer producer1 = session1.createProducer(queue[1]);

         producer1.setDeliveryMode(DeliveryMode.PERSISTENT);

         //Send messages 10 - 19 on node 1
         for (int i = 10; i < 20; i++)
         {
            producer1.send(session0.createTextMessage("message " + i));
         }

         session1.commit();
         
         log.info("Sent messages 10 - 19 on node 1");
         
         //At this point we should have messages 5 - 9 sitting in queue on node 0
         //and messages 10 - 19 sitting in queue on node 1

         //Create a consumer on node 1
         
         log.info("Creating consumer on node 1");
         
         // This will actually end up sucking messages from node 0
         MessageConsumer consumer1 = session1.createConsumer(queue[1]);
         
         //Give it enough time to suck
         
         log.info("Waiting for suck");
         Thread.sleep(5000);

         log.info("Killing node1");
         ServerManagement.kill(1);
         log.info("Killed node1");

         // close the consumer .. .and this should cause failover to kick in
         log.info("closing the consumer");
         consumer1.close();
         
         log.info("closed the consumer");

         consumer0 = session0.createConsumer(queue[0]);
         
         log.info("creating new consumer");
         
         //We should now be able to consume the messages 5 to 19.
         //Note that they will be in a different order since 10 to 10 were sucked to node 0 before crashing
         //Also there is the possibility that after crashing the queue attempted to delivery to one or more of the remote consumers
         //for the node that crashed, (YES it is possible to send more than one message on a failed connection before getting
         //an exception), so this won't be cancelled until the connection checker kicks in any closes the consumer         
         
         Set msgs = new HashSet();
         
         for (int i = 5; i < 20; i++)
         {
            msg = (TextMessage)consumer0.receive(60000);
            assertNotNull(msg);
            
            log.info("Got message " + msg.getText());
            
            msgs.add(msg.getText());
         }
         
         for (int i = 5; i < 20; i++)
         {
            assertTrue(msgs.contains("message " + i));
         }

         assertNull(consumer0.receive(5000));

         session0.commit();
         
         log.info("end");
      }
      finally
      {
         if (conn0!=null)
         {
            conn0.close();
         }

         if (conn1!=null)
         {
            conn1.close();
         }
      }
   }

   public void testMergeQueueSimple() throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;

      try
      {
         // Objects Server0
         conn0 = createConnectionOnServer(cf, 0);

         assertEquals(0, getServerId(conn0));
         
         conn1 = createConnectionOnServer(cf, 1);
         
         assertEquals(1, getServerId(conn1));
         
         //Send some messages on node 0
         
         Session session0 = conn0.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer producer0 = session0.createProducer(queue[0]);

         for (int i = 0; i < 10; i++)
         {
            producer0.send(session0.createTextMessage("message " + i));
         }
         
         session0.commit();
         
         
         //Send some more on node 1
         
         Session session1 = conn1.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer producer1 = session1.createProducer(queue[1]);

         for (int i = 10; i < 20; i++)
         {
            producer1.send(session1.createTextMessage("message " + i));
         }
         
         session1.commit();
         
         
         //Don't consume them or they will be pulled from one node to another
         
         
         //Now kill the server
         waitForFailoverComplete(1, conn1);

         //Messages should all be available on node 0
         
         MessageConsumer cons0 = session0.createConsumer(queue[0]);
         
         TextMessage tm;
         
         conn0.start();
         
         for (int i = 0; i < 20; i++)
         {
            tm = (TextMessage)cons0.receive(60000);
            
            assertNotNull(tm);
            
            log.info("received message " + tm.getText());
                        
            assertEquals("message " + i, tm.getText());
         }
         
         tm = (TextMessage)cons0.receive(1000);
         
         assertNull(tm);
         
         session0.commit();
         
      }
      finally
      {
         if (conn0!=null)
         {
            conn0.close();
         }

         if (conn1!=null)
         {
            conn1.close();
         }
      }
   }
   // Fil consumer
   
   /*
    * Both queues paging > fullsize
    */
   public void testMergeQueuePagingFill1() throws Exception
   {      
      mergeQueuePaging(20, 20, 10, 10, true);
   }
   
   /*
    * Both queues paging = fullsize
    */
   public void testMergeQueuePagingFill2() throws Exception
   {
      mergeQueuePaging(10, 10, 10, 10, true);
   }
   
   /*
    * First queue paging, second queue not > full size
    */
   public void testMergeQueuePagingFill3() throws Exception
   {
      mergeQueuePaging(20, 5, 10, 10, true);
   }
   
   /*
    * Second queue paging, first queue not > full size
    */
   public void testMergeQueuePagingFill4() throws Exception
   {
      mergeQueuePaging(5, 20, 10, 10, true);
   }
   
   /*
    * First queue paging, second queue not = full size
    */
   public void testMergeQueuePagingFill5() throws Exception
   {
      mergeQueuePaging(10, 5, 10, 10, true);
   }
   
   /*
    * Second queue paging, first queue not = full size
    */
   public void testMergeQueuePagingFill6() throws Exception
   {
      mergeQueuePaging(5, 10, 10, 10, true);
   }
   
   // Don't fill consumer
   
   /*
    * Both queues paging > fullsize
    */
   public void testMergeQueuePagingNoFill1() throws Exception
   {      
      mergeQueuePaging(20, 20, 10, 10, false);
   }
   
   /*
    * Both queues paging = fullsize
    */
   public void testMergeQueuePagingNoFill2() throws Exception
   {
      mergeQueuePaging(10, 10, 10, 10, false);
   }
   
   /*
    * First queue paging, second queue not > full size
    */
   public void testMergeQueuePagingNoFill3() throws Exception
   {
      mergeQueuePaging(20, 5, 10, 10, false);
   }
   
   /*
    * Second queue paging, first queue not > full size
    */
   public void testMergeQueuePagingNoFill4() throws Exception
   {
      mergeQueuePaging(5, 20, 10, 10, false);
   }
   
   /*
    * First queue paging, second queue not = full size
    */
   public void testMergeQueuePagingNoFill5() throws Exception
   {
      mergeQueuePaging(10, 5, 10, 10, false);
   }
   
   /*
    * Second queue paging, first queue not = full size
    */
   public void testMergeQueuePagingNoFill6() throws Exception
   {
      mergeQueuePaging(5, 10, 10, 10, false);
   }
   
   /*
    * Both queues paging on merge
    */
   private void mergeQueuePaging(int messages0, int messages1, int full0, int full1, boolean fillConsumer) throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;

      try
      {
         //Deploy queue with fullSize of 10
         
         ServerManagement.deployQueue("constrainedQueue", "queue/constrainedQueue",full0, 2, 2, 0, true);
         
         ServerManagement.deployQueue("constrainedQueue", "queue/constrainedQueue",full1, 2, 2, 1, true);
         
         Queue queue0 = (Queue)ic[0].lookup("queue/constrainedQueue");
         
         Queue queue1 = (Queue)ic[1].lookup("queue/constrainedQueue");
         
         // Objects Server0
         conn0 = createConnectionOnServer(cf, 0);

         assertEquals(0, getServerId(conn0));
         
         conn1 = createConnectionOnServer(cf, 1);
         
         assertEquals(1, getServerId(conn1));
         
         //Send some messages on node 0
         
         Session session0 = conn0.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer producer0 = session0.createProducer(queue0);

         log.info("sending messages on node 0");
         
         for (int i = 0; i < messages0; i++)
         {
            producer0.send(session0.createTextMessage("message " + i));
            
            log.info("Sent message: message " + i);
         }
         
         session0.commit();
         
         
         //Send some more on node 1
         
         log.info("Sending some messages on node 1");
         
         Session session1 = conn1.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer producer1 = session1.createProducer(queue1);

         for (int i = messages0; i < messages0 + messages1; i++)
         {
            producer1.send(session1.createTextMessage("message " + i));
            
            log.info("Sent message: message " + i);
         }
         
         session1.commit();
         
         MessageConsumer cons0 = null;
         
         if (fillConsumer)
         {
            //Creating the consumer immediately after kill should ensure that all the messages are in the consumer and
            //not paged to disk
            cons0 = session0.createConsumer(queue0);
         }
                 
         //Now kill the server

         waitForFailoverComplete(1, conn1);
         
         if (!fillConsumer)
         {
            cons0 = session0.createConsumer(queue0);
         }

         //Messages should all be available on node 0
         
         //Note they may be in a different order due to being pulled in to the consumer before killing the server
         //And also because they may have been attempted to have been delivered to a remote consumer corresponding to a
         //remote consumer for the failed node, so that delivery or one after may fail, so those messages may not get cancelled
         //back until the connection checker kicks in and closes the consumer
         
         conn0.start();                 
                 
         
         Set msgs = new HashSet();
         
         TextMessage tm;
         
         for (int i = 0; i < messages0 + messages1; i++)
         {
         	//Need a long timeout to allow for connection checker to kick in and close consumer
         	tm = (TextMessage)cons0.receive(60000);
            
            assertNotNull(tm);
            
            log.info("Got message " + tm.getText());
            
            msgs.add(tm.getText());
         }
         
         for (int i = 0; i < messages0 + messages1; i++)
         {
            assertTrue(msgs.contains("message " + i));
         }
                  
         tm = (TextMessage)cons0.receive(2000);
         
         assertNull(tm);    
         
         session0.commit();
      }
      finally
      {
         try
         {
            ServerManagement.undeployQueue("constrainedQueue", 0);
         }
         catch (Exception ignore)
         {            
         }
         
         try
         {
            ServerManagement.undeployQueue("constrainedQueue", 1);
         }
         catch (Exception ignore)
         {            
         }
                  
         if (conn0!=null)
         {
            conn0.close();
         }

         if (conn1!=null)
         {
            conn1.close();
         }
      }
   }
   
   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 2;

      super.setUp();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
   
}
