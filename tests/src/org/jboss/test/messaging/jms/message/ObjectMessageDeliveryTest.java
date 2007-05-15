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
package org.jboss.test.messaging.jms.message;

import java.io.Serializable;

import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * 
 * A ObjectMessageDeliveryTest
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ObjectMessageDeliveryTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext ic;
   protected TopicConnectionFactory cf;

   // Constructors --------------------------------------------------
   
   public ObjectMessageDeliveryTest(String name)
   {
      super(name);
   }
   
   // TestCase overrides -------------------------------------------
   
   public void setUp() throws Exception
   {
      super.setUp();                  
      
      ServerManagement.start("all");
      
      
      ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
      
      ServerManagement.undeployTopic("TestTopic");
      ServerManagement.deployTopic("TestTopic");

      log.debug("setup done");
   }
   
   public void tearDown() throws Exception
   {
      ServerManagement.undeployTopic("TestTopic");
      
      super.tearDown();

      log.debug("tear down done");
   }

   // Public --------------------------------------------------------

   
   static class TestObject implements Serializable
   {
		private static final long serialVersionUID = -340663970717491155L;
		String text;
   }
   
   /**
    * 
    */
   public void testTopic() throws Exception
   {
      Topic topic = (Topic)ic.lookup("/topic/TestTopic");

      TopicConnection conn = cf.createTopicConnection();

      try
      {
         TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         TopicPublisher publisher = s.createPublisher(topic);
         TopicSubscriber sub = s.createSubscriber(topic);
         conn.start();
                  
         //Create 3 object messages with different bodies
         
         TestObject to1 = new TestObject();
         to1.text = "hello1";
         
         TestObject to2 = new TestObject();
         to1.text = "hello2";
         
         TestObject to3 = new TestObject();
         to1.text = "hello3";
         
         ObjectMessage om1 = s.createObjectMessage();
         om1.setObject(to1);
         
         ObjectMessage om2 = s.createObjectMessage();
         om2.setObject(to2);
         
         ObjectMessage om3 = s.createObjectMessage();
         om3.setObject(to3);
         
         //send to topic
         publisher.send(om1);
         
         publisher.send(om2);
         
         publisher.send(om3);
         
         ObjectMessage rm1 = (ObjectMessage)sub.receive(MAX_TIMEOUT);
         
         ObjectMessage rm2 = (ObjectMessage)sub.receive(MAX_TIMEOUT);
         
         ObjectMessage rm3 = (ObjectMessage)sub.receive(MAX_TIMEOUT);
         
         assertNotNull(rm1);
         
         TestObject ro1 = (TestObject)rm1.getObject();
         
         assertEquals(to1.text, ro1.text);assertNotNull(rm1);
         
         TestObject ro2 = (TestObject)rm2.getObject();
         
         assertEquals(to2.text, ro2.text);
         
         assertNotNull(rm2);
         
         TestObject ro3 = (TestObject)rm3.getObject();
         
         assertEquals(to3.text, ro3.text);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}


