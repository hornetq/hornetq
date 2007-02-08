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
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.message.MessageProxy;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.message.SimpleMessageReference;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * A ReferencingTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ReferencingTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   
   protected Destination queue;

   // Constructors --------------------------------------------------

   public ReferencingTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("this test is not supposed to run in a remote configuration!");
      }

      super.setUp();
      ServerManagement.start("all");
            
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");

      queue = (Destination)initialContext.lookup("/queue/Queue"); 
      
      this.drainDestination(cf, queue);
      
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
        
      super.tearDown();
   }


   // Public --------------------------------------------------------
   
   public void testAutoAck1() throws Exception
   {
      MessageStore store = ServerManagement.getMessageStore();
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
      
      MessageConsumer cons = sess.createConsumer(queue);
      
      conn.start();
      
      TextMessage m = sess.createTextMessage("wibble");
      
      prod.send(m);
  
      TextMessage m2 = (TextMessage)cons.receive();
       
      assertNotNull(m2);
      assertEquals(m.getText(), m2.getText());
      
      MessageReference ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertNull(ref);
      
      conn.close();
   }
   
   public void testAutoAck2() throws Exception
   {
      MessageStore store = ServerManagement.getMessageStore();
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
                  
      conn.start();
      
      TextMessage m = sess.createTextMessage("wibble");
      
      prod.send(m);
      
      Thread.sleep(1000);
      
      MessageConsumer cons = sess.createConsumer(queue);
      
      TextMessage m2 = (TextMessage)cons.receive(1000);
      
      
      assertNotNull(m2);
      
      assertEquals(m.getText(), m2.getText());
      
      MessageReference ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertNull(ref);
      
      conn.close();
   }
   
   public void testClientAck1() throws Exception
   {
      MessageStore store = ServerManagement.getMessageStore();
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
      
      MessageConsumer cons = sess.createConsumer(queue);
      
      conn.start();
      
      TextMessage m = sess.createTextMessage("wibble");
      
      prod.send(m);
      
      TextMessage m2 = (TextMessage)cons.receive(1000);
      
      assertNotNull(m2);
      assertEquals(m.getText(), m2.getText());
      
      MessageReference ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertEquals(((MessageProxy)m).getMessage().getMessageID(), ref.getMessage().getMessageID());
      
      ref.releaseMemoryReference();
          
      m2.acknowledge();
      
      ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertNull(ref);
      
      conn.close();
   }
   
   public void testClientAck2() throws Exception
   {
      MessageStore store = ServerManagement.getMessageStore();
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
                  
      conn.start();
      
      TextMessage m = sess.createTextMessage("wibble");
      
      prod.send(m);
      
      Thread.sleep(1000);
      
      MessageConsumer cons = sess.createConsumer(queue);
      
      TextMessage m2 = (TextMessage)cons.receive(1000);
      
      assertNotNull(m2);
      assertEquals(m.getText(), m2.getText());
      
      MessageReference ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertEquals(((MessageProxy)m).getMessage().getMessageID(), ref.getMessage().getMessageID());
      
      ref.releaseMemoryReference();
      
      m2.acknowledge();
      
      ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertNull(ref);
      
      conn.close();
   }
   
   public void testRedelivery() throws Exception
   {
      MessageStore store = ServerManagement.getMessageStore();
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
                  
      conn.start();
      
      TextMessage m = sess.createTextMessage("wibble");
      
      prod.send(m);
      
      Thread.sleep(1000);
      
      MessageConsumer cons = sess.createConsumer(queue);
      
      TextMessage m2 = (TextMessage)cons.receive(1000);
      
      assertNotNull(m2);
      assertEquals(m.getText(), m2.getText());
      
      MessageReference ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertEquals(((MessageProxy)m).getMessage().getMessageID(), ref.getMessage().getMessageID());
      
      ref.releaseMemoryReference();
      
      sess.recover();
      
      TextMessage m3 = (TextMessage)cons.receive();
      
      assertNotNull(m3);
      assertEquals(m.getText(), m3.getText());
      
      ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertEquals(((MessageProxy)m).getMessage().getMessageID(), ref.getMessage().getMessageID());
      
      ref.releaseMemoryReference();
            
      m2.acknowledge();
      
      ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertNull(ref);
      
      conn.close();
   }
   
   public void testTransactionCommit() throws Exception
   {
      MessageStore store = ServerManagement.getMessageStore();
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageProducer prod = sess.createProducer(queue);
      
      MessageConsumer cons = sess.createConsumer(queue);
                        
      conn.start();
      
      TextMessage m = sess.createTextMessage("wibble");
      
      prod.send(m);
      sess.commit();
      
      TextMessage m2 = (TextMessage)cons.receive(1000);
      
      assertNotNull(m2);
      assertEquals(m.getText(), m2.getText());
      
      SimpleMessageReference ref = (SimpleMessageReference)store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertEquals(2, ref.getInMemoryChannelCount());
      
      assertEquals(((MessageProxy)m).getMessage().getMessageID(), ref.getMessage().getMessageID());
      
      ref.releaseMemoryReference();
      
      sess.commit();
      
      ref = (SimpleMessageReference)store.reference(((MessageProxy)m2).getMessage().getMessageID());
      assertNull(ref);
      
      conn.close();
   }
   
   public void testTransactionRollback() throws Exception
   {
      MessageStore store = ServerManagement.getMessageStore();
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      
      MessageProducer prod = sess.createProducer(queue);
      
      MessageConsumer cons = sess.createConsumer(queue);
                        
      conn.start();
      
      TextMessage m = sess.createTextMessage("wibble");
      
      prod.send(m);
      sess.commit();
      
      TextMessage m2 = (TextMessage)cons.receive(1000);
      
      assertNotNull(m2);
      assertEquals(m.getText(), m2.getText());
      
      MessageReference ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      assertEquals(((MessageProxy)m).getMessage().getMessageID(), ref.getMessage().getMessageID());
      
      ref.releaseMemoryReference();
      
      sess.rollback();
      
      TextMessage m3 = (TextMessage)cons.receive();
      
      assertNotNull(m3);
      assertEquals(m.getText(), m3.getText());
      
      ref = store.reference(((MessageProxy)m3).getMessage().getMessageID());
      assertEquals(((MessageProxy)m).getMessage().getMessageID(), ref.getMessage().getMessageID());
      
      ref.releaseMemoryReference();
      
      sess.commit();
      
      ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      assertNull(ref);
      
      conn.close();
   }
   
   public void cancelTest() throws Exception
   {
      MessageStore store = ServerManagement.getMessageStore();
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
      
      MessageConsumer cons = sess.createConsumer(queue);
      
      conn.start();
      
      TextMessage m1 = sess.createTextMessage("wibble");
      TextMessage m2 = sess.createTextMessage("wibble");
      
      prod.send(m1);
      prod.send(m2);
      
      TextMessage m3 = (TextMessage)cons.receive(1000);
      
      assertNotNull(m3);
      assertEquals(m1.getText(), m3.getText());
      
      TextMessage m4 = (TextMessage)cons.receive();
      
      assertNotNull(m4);
      assertEquals(m1.getText(), m4.getText());
      
      sess.recover();
      
      TextMessage m5 = (TextMessage)cons.receive();
      
      assertNotNull(m5);
      assertEquals(m1.getText(), m5.getText());
      
      TextMessage m6 = (TextMessage)cons.receive();
      
      assertNotNull(m6);
      assertEquals(m1.getText(), m6.getText()); 
      
      m6.acknowledge();
      
      MessageReference ref = store.reference(((MessageProxy)m2).getMessage().getMessageID());
      
      assertNull(ref);
      
      conn.close();
   }
   
      
}


