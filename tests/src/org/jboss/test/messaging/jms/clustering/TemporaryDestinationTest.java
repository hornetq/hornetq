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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;


/**
 * A test for temporary destinations in a clustered enviroment.
 * See http://jira.jboss.org/jira/browse/JBMESSAGING-841.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TemporaryDestinationTest extends ClusteringTestBase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public TemporaryDestinationTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testTemporaryQueueTrivial() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = session.createTemporaryQueue();

         MessageProducer prod = session.createProducer(queue);
         prod.send(session.createTextMessage("kwijibo"));

         MessageConsumer cons = session.createConsumer(queue);
         conn.start();

         TextMessage rm = (TextMessage)cons.receive(2000);

         assertNotNull(rm);
         assertEquals("kwijibo", rm.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testTemporaryQueue() throws Exception
   {
   	Connection conn0 = null;
   	
   	Connection conn1 = null;

      try
      {
         conn0 = cf.createConnection();
         
         conn1 = cf.createConnection();
         
         // Make sure the connections are on different servers
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1});
         
         Session session0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue queue = session0.createTemporaryQueue();
         
         MessageConsumer cons0 = session0.createConsumer(queue);
         
         conn0.start();
         
         //The second connection sends a message back to the temporary queue from a different node
         //This is what would happen in the classic "replyTo queue pattern"
         //In the clustered version of this pattern the message can be sent to the replyTo queue from any node
         //in the cluster
         
         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = session1.createProducer(queue);
         
         TextMessage sm = session1.createTextMessage("hoo ja ma flip");
         
         log.info("Sending message!");
         prod.send(sm);
         
         TextMessage tm = (TextMessage)cons0.receive(3000);
         
         assertNotNull(tm);
         
         assertEquals(sm.getText(), tm.getText());
         
         log.info("Received message!");
             
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
   
   public void testTemporaryTopic() throws Exception
   {
   	Connection conn0 = null;
   	
   	Connection conn1 = null;

      try
      {
         conn0 = cf.createConnection();
         
         conn1 = cf.createConnection();
         
         // Make sure the connections are on different servers
         
         checkConnectionsDifferentServers(new Connection[] {conn0, conn1});
         
         Session session0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Topic topic = session0.createTemporaryTopic();
         
         Thread.sleep(1000);
         
         MessageConsumer cons0 = session0.createConsumer(topic);
         
         conn0.start();
         
         //The second connection sends a message back to the temporary topic from a different node
         //This is what would happen in the classic "replyTo topic pattern"
         //In the clustered version of this pattern the message can be sent to the replyTo topic from any node
         //in the cluster
         
         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = session1.createProducer(topic);
         
         TextMessage sm = session1.createTextMessage("hoo ja ma flip2");
         
         prod.send(sm);
         
         TextMessage tm = (TextMessage)cons0.receive(3000);
         
         assertNotNull(tm);
         
         assertEquals(sm.getText(), tm.getText());
             
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
   

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 2;

      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
