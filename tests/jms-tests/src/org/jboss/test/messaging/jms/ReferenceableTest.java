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
package org.jboss.test.messaging.jms;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * 
 * A ReferenceableTest.
 * 
 * All administered objects should be referenceable and serializable as per spec 4.2
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ReferenceableTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testSerializable() throws Exception
   {
      assertTrue(cf instanceof Serializable);
      
      assertTrue(queue1 instanceof Serializable);
      
      assertTrue(topic1 instanceof Serializable);            
   }

   /* http://jira.jboss.org/jira/browse/JBMESSAGING-395

   public void testReferenceable() throws Exception
   {
      assertTrue(cf instanceof Referenceable);
      
      assertTrue(queue instanceof Referenceable);
      
      assertTrue(topic instanceof Referenceable);
   }
   
   public void testReferenceCF() throws Exception
   {
      Reference cfRef = ((Referenceable)cf).getReference();
      
      String factoryName = cfRef.getFactoryClassName();
      
      Class factoryClass = Class.forName(factoryName);
      
      ConnectionFactoryObjectFactory factory = (ConnectionFactoryObjectFactory)factoryClass.newInstance();
      
      Object instance = factory.getObjectInstance(cfRef, null, null, null);
      
      assertTrue(instance instanceof JBossConnectionFactory);
      
      JBossConnectionFactory cf2 = (JBossConnectionFactory)instance;
      
      simpleSendReceive(cf2, queue);
   }
   
   public void testReferenceQueue() throws Exception
   {
      Reference queueRef = ((Referenceable)queue).getReference();
      
      String factoryName = queueRef.getFactoryClassName();
      
      Class factoryClass = Class.forName(factoryName);
      
      DestinationObjectFactory factory = (DestinationObjectFactory)factoryClass.newInstance();
      
      Object instance = factory.getObjectInstance(queueRef, null, null, null);
      
      assertTrue(instance instanceof JBossQueue);
      
      JBossQueue queue2 = (JBossQueue)instance;
      
      assertEquals(queue.getQueueName(), queue2.getQueueName());
      
      simpleSendReceive(cf, queue2);
      
   }
   
   public void testReferenceTopic() throws Exception
   {
      Reference topicRef = ((Referenceable)topic).getReference();
      
      String factoryName = topicRef.getFactoryClassName();
      
      Class factoryClass = Class.forName(factoryName);
      
      DestinationObjectFactory factory = (DestinationObjectFactory)factoryClass.newInstance();
      
      Object instance = factory.getObjectInstance(topicRef, null, null, null);
      
      assertTrue(instance instanceof JBossTopic);
      
      JBossTopic topic2 = (JBossTopic)instance;
      
      assertEquals(topic.getTopicName(), topic2.getTopicName());
      
      simpleSendReceive(cf, topic2);
   }

   */
   
   
   protected void simpleSendReceive(ConnectionFactory cf, Destination dest) throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      MessageProducer prod = sess.createProducer(dest);
	      
	      MessageConsumer cons = sess.createConsumer(dest);
	      
	      conn.start();
	      
	      TextMessage tm = sess.createTextMessage("ref test");
	      
	      prod.send(tm);
	      
	      tm = (TextMessage)cons.receive(1000);
	      
	      assertNotNull(tm);
	      
	      assertEquals("ref test", tm.getText());
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }
}



