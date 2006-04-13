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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class SubscriptionWithSelectorTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Topic topic;

   // Constructors --------------------------------------------------

   public SubscriptionWithSelectorTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
                  
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
                 
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      topic = (Topic)initialContext.lookup("/topic/Topic");     

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployTopic("Topic");
      
      super.tearDown();
      
   }
   public void testWithSelector() throws Exception
   {
      String selector1 = "beatle = 'john'";
      String selector2 = "beatle = 'paul'";
      String selector3 = "beatle = 'george'";
      String selector4 = "beatle = 'ringo'";
      String selector5 = "beatle = 'jesus'";
      
      Connection conn = cf.createConnection();
      conn.start();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons1 = sess.createConsumer(topic, selector1);
      MessageConsumer cons2 = sess.createConsumer(topic, selector2);
      MessageConsumer cons3 = sess.createConsumer(topic, selector3);
      MessageConsumer cons4 = sess.createConsumer(topic, selector4);
      MessageConsumer cons5 = sess.createConsumer(topic, selector5);
      
      Message m1 = sess.createMessage();
      m1.setStringProperty("beatle", "john");
      
      Message m2 = sess.createMessage();
      m2.setStringProperty("beatle", "paul");
      
      Message m3 = sess.createMessage();
      m3.setStringProperty("beatle", "george");
      
      Message m4 = sess.createMessage();
      m4.setStringProperty("beatle", "ringo");
      
      Message m5 = sess.createMessage();
      m5.setStringProperty("beatle", "jesus");
      
      MessageProducer prod = sess.createProducer(topic);
      
      prod.send(m1);
      prod.send(m2);
      prod.send(m3);
      prod.send(m4);
      prod.send(m5);
      
      Message r1 = cons1.receive(500);
      assertNotNull(r1);
      Message n = cons1.receive(500);
      assertNull(n);
      
      Message r2 = cons2.receive(500);
      assertNotNull(r2);
      n = cons2.receive(500);
      assertNull(n);
      
      Message r3 = cons3.receive(500);
      assertNotNull(r3);
      n = cons3.receive(500);
      assertNull(n);
      
      Message r4 = cons4.receive(500);
      assertNotNull(r4);
      n = cons4.receive(500);
      assertNull(n);
      
      Message r5 = cons5.receive(500);
      assertNotNull(r5);
      n = cons5.receive(500);
      assertNull(n);
      
      assertEquals("john", r1.getStringProperty("beatle"));
      assertEquals("paul", r2.getStringProperty("beatle"));
      assertEquals("george", r3.getStringProperty("beatle"));
      assertEquals("ringo", r4.getStringProperty("beatle"));
      assertEquals("jesus", r5.getStringProperty("beatle"));
      
      conn.close();
            
   }


   // Public --------------------------------------------------------
   
}



