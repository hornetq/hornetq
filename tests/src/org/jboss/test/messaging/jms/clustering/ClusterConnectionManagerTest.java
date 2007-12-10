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

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.jboss.test.messaging.tools.ServerManagement;

/**
 *
 * We test every combination of the order of deployment of connection factory, local and remote queue
 *
 * and verify message sucking still works
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>2 Jul 2007
 *
 * $Id: $
 *
 */
public class ClusterConnectionManagerTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClusterConnectionManagerTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void test1() throws Exception
   {
   	deployCFLocal();
   	deployCFRemote();
   	deployLocal();
   	deployRemote();
   	suck();
   }

   public void test2() throws Exception
   {
   	deployCFLocal();
   	deployCFRemote();
   	deployRemote();
   	deployLocal();
   	suck();
   }

   public void test3() throws Exception
   {
   	deployCFLocal();
   	deployLocal();
   	deployCFRemote();
   	deployRemote();
   	suck();
   }

   public void test4() throws Exception
   {
   	deployCFLocal();
   	deployLocal();
   	deployRemote();
   	deployCFRemote();
   	suck();
   }

   public void test5() throws Exception
   {
   	deployCFLocal();
   	deployRemote();
   	deployCFRemote();
   	deployLocal();
   	suck();
   }

   public void test6() throws Exception
   {
   	deployCFLocal();
   	deployRemote();
   	deployLocal();
   	deployCFRemote();
   	suck();
   }

   public void test7() throws Exception
   {
   	deployCFRemote();
   	deployCFLocal();
   	deployLocal();
   	deployRemote();
   	suck();
   }

   public void test8() throws Exception
   {
   	deployCFRemote();
   	deployCFLocal();
   	deployRemote();
   	deployLocal();
   	suck();
   }

   public void test9() throws Exception
   {
   	deployCFRemote();
   	deployLocal();
   	deployCFLocal();
   	deployRemote();
   	suck();
   }

   public void test10() throws Exception
   {
   	deployCFRemote();
   	deployLocal();
   	deployRemote();
   	deployCFLocal();
   	suck();
   }

   public void test11() throws Exception
   {
   	deployCFRemote();
   	deployRemote();
   	deployCFLocal();
   	deployLocal();
   	suck();
   }

   public void test12() throws Exception
   {
   	deployCFRemote();
   	deployRemote();
   	deployLocal();
   	deployCFLocal();
   	suck();
   }

   public void test13() throws Exception
   {
   	deployLocal();
   	deployCFLocal();
   	deployCFRemote();
   	deployRemote();
   	suck();
   }

   public void test14() throws Exception
   {
   	deployLocal();
   	deployCFLocal();
   	deployRemote();
   	deployCFRemote();
   	suck();
   }

   public void test15() throws Exception
   {
   	deployLocal();
   	deployCFRemote();
   	deployCFLocal();
   	deployRemote();
   	suck();
   }

   public void test16() throws Exception
   {
   	deployLocal();
   	deployCFRemote();
   	deployRemote();
   	deployCFLocal();
   	suck();
   }

   public void test17() throws Exception
   {
   	deployLocal();
   	deployRemote();
   	deployCFLocal();
   	deployCFRemote();
   	suck();
   }

   public void test18() throws Exception
   {
   	deployLocal();
   	deployRemote();
   	deployCFRemote();
   	deployCFLocal();
   	suck();
   }

   public void test19() throws Exception
   {
   	deployRemote();
   	deployCFLocal();
   	deployCFRemote();
   	deployLocal();
   	suck();
   }

   public void test20() throws Exception
   {
   	deployRemote();
   	deployCFLocal();
   	deployLocal();
   	deployCFRemote();
   	suck();
   }

   public void test21() throws Exception
   {
   	deployRemote();
   	deployCFRemote();
   	deployCFLocal();
   	deployLocal();
   	suck();
   }

   public void test22() throws Exception
   {
   	deployRemote();
   	deployCFRemote();
   	deployLocal();
   	deployCFLocal();
   	suck();
   }

   public void test23() throws Exception
   {
   	deployRemote();
   	deployLocal();
   	deployCFLocal();
   	deployCFRemote();
   	suck();
   }

   public void test24() throws Exception
   {
   	deployRemote();
   	deployLocal();
   	deployCFRemote();
   	deployCFLocal();
   	suck();
   }

   // http://jira.jboss.org/jira/browse/JBMESSAGING-1136
   public void testCreateConsumerBeforeRemoteDeployment() throws Exception
   {
      final int NUM_MESSAGES = 20;

      deployCFLocal();
      deployLocal();

      //Send some messages

      Queue queue0 = (Queue)ic[0].lookup("/queue/suckQueue");

      Connection conn0 = null;

      try
      {
         conn0 = this.createConnectionOnServer(cf, 0);

         assertEquals(0, getServerId(conn0));

         //Send some messages on node 0

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess0.createProducer(queue0);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess0.createTextMessage("message" + i);

            prod.send(tm);
         }
      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }
      }

      log.info("Sent messages");

      //Undeploy
      this.undeployAll();

      log.info("Undeployed");

      deployCFRemote();
      deployRemote();

      Queue queue1 = (Queue)ic[1].lookup("/queue/suckQueue");

      //Create the consumer - but the messages will be stranded on other node
      //Until we deploy - we do this on another thread

      Thread t = new Thread(new Runnable() {
         public void run()
         {
            try
            {
               Thread.sleep(5000);
               deployCFLocal();
               deployLocal();
            }
            catch (Exception e)
            {
               log.error("Failed to deploy", e);
            }
         }
      });

      t.start();

      Connection conn1 = null;

      try
      {
         //Consume them on node 1

         conn1 = this.createConnectionOnServer(cf, 1);

         assertEquals(1, getServerId(conn1));

         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons1 = sess1.createConsumer(queue1);

         conn1.start();

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage)cons1.receive(30000);

            assertNotNull(tm);

            log.info("Got message " + tm.getText());

            assertEquals("message" + i, tm.getText());
         }
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }

      t.join();

   }
   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      undeployAll();
   }

   protected void tearDown() throws Exception
   {
   	super.tearDown();

   	undeployAll();
   }

   private void undeployAll() throws Exception
   {
   	undeployQueue("suckQueue", 0);

   	undeployQueue("suckQueue", 1);
   	String cfName = getJmsServer().getConfiguration().getClusterPullConnectionFactoryName();

      log.info("CF name is " + cfName);


   	//undeploy cf on node 0
      try
      {
      	undeployConnectionFactory(0,cfName);
      }
      catch (Exception ignore)
      {
         //ignore.printStackTrace();
      }

   	//undeploy cf on node 0
      try
      {
      	undeployConnectionFactory(1, cfName);
      }
      catch (Exception ignore)
      {
         //ignore.printStackTrace();
      }
   }

   // Private --------------------------------------------------------------------------------------

   private void deployCFRemote() throws Exception
   {
   	String cfName = getJmsServer().getConfiguration().getClusterPullConnectionFactoryName();
   	//Deploy cf on node 1
   	deployConnectionFactory(1, cfName, null, 150);
   }

   private void deployCFLocal() throws Exception
   {
   	String cfName = getJmsServer().getConfiguration().getClusterPullConnectionFactoryName();
   	//Deploy cf on node 0
   	deployConnectionFactory(0,cfName, null, 150);
   }

   private void deployLocal() throws Exception
   {
   	 deployQueue("suckQueue", 0);
   }

   private void deployRemote() throws Exception
   {
   	 deployQueue("suckQueue", 1);
   }

   private void suck() throws Exception
   {
      Queue queue0 = (Queue)ic[0].lookup("/queue/suckQueue");

      Queue queue1 = (Queue)ic[1].lookup("/queue/suckQueue");

      Connection conn0 = null;

      Connection conn1 = null;

      try
      {
      	conn0 = this.createConnectionOnServer(cf, 0);

      	assertEquals(0, getServerId(conn0));

      	//Send some messages on node 0

      	final int NUM_MESSAGES = 20;

      	Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);

      	MessageProducer prod = sess0.createProducer(queue0);

      	//Note! The message must be sent as non persistent for this test
      	//Since we have not deployed suckQueue on all nodes of the cluster
      	//this would cause persistent messages to not be delivered since they would
      	//fail to replicate to their backup (since suckQueue is not deployed on it)

      	prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      	
      	for (int i = 0; i < NUM_MESSAGES; i++)
      	{
      		TextMessage tm = sess0.createTextMessage("message" + i);
      		
      		prod.send(tm);
      	}
      	
      	//Consume them on node 1
      	
      	conn1 = this.createConnectionOnServer(cf, 1);
      	
      	assertEquals(1, getServerId(conn1));
      	
      	Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      	
      	MessageConsumer cons1 = sess1.createConsumer(queue1);
      	
      	conn1.start();
      	
      	for (int i = 0; i < NUM_MESSAGES; i++)
      	{
      		TextMessage tm = (TextMessage)cons1.receive(5000);
      		
      		assertNotNull(tm);
      		
      		assertEquals("message" + i, tm.getText());
      	} 
      }
      finally
      {
      	if (conn0 != null)
      	{
      		conn0.close();
      	}
      	if (conn1 != null)
      	{
      		conn1.close();
      	}
      }
      
   }



   // Inner classes --------------------------------------------------------------------------------

}
