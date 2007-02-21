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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.messaging.util.SafeUTF;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * There is a bug in JDK1.3, 1.4 whereby writeUTF fails if more than 64K bytes are written
 * we need to work with all size of strings
 * 
 * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4806007
 * http://jira.jboss.com/jira/browse/JBAS-2641
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class String64KLimitTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;

   // Constructors --------------------------------------------------

   public String64KLimitTest(String name)
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
                 
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      queue = (Destination)initialContext.lookup("/queue/Queue"); 

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
      
      super.tearDown();
      
   }


   // Public --------------------------------------------------------
   
   public void testSafeWriteReadUTFTest1() throws Exception
   {
      //Make sure we hit all the edge cases
      doTest("a", 10);
      doTest("ab", 10);
      doTest("abc", 10);
      doTest("abcd", 10);
      doTest("abcde", 10);
      doTest("abcdef", 10);
      doTest("abcdefg", 10);
      doTest("abcdefgh", 10);
      doTest("abcdefghi", 10);
      doTest("abcdefghij", 10);
      doTest("abcdefghijk", 10);
      doTest("abcdefghijkl", 10);
      doTest("abcdefghijklm", 10);
      doTest("abcdefghijklmn", 10);
      doTest("abcdefghijklmno", 10);
      doTest("abcdefghijklmnop", 10);
      doTest("abcdefghijklmnopq", 10);
      doTest("abcdefghijklmnopqr", 10);
      doTest("abcdefghijklmnopqrs", 10);
      doTest("abcdefghijklmnopqrst", 10);
      doTest("abcdefghijklmnopqrstu", 10);
      doTest("abcdefghijklmnopqrstuv", 10);
      doTest("abcdefghijklmnopqrstuvw", 10);
      doTest("abcdefghijklmnopqrstuvwx", 10);
      doTest("abcdefghijklmnopqrstuvwxy", 10);
      doTest("abcdefghijklmnopqrstuvwxyz", 10);
      
      //Need to work with null too
      doTest(null, 10);
              
   }
   
   protected void doTest(String s, int chunkSize) throws Exception
   {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      
      DataOutputStream dos = new DataOutputStream(bos);
      
      SafeUTF su = new SafeUTF(chunkSize);
      
      su.safeWriteUTF(dos, s);
      
      dos.close();
      
      byte[] bytes = bos.toByteArray();
      
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      
      DataInputStream dis = new DataInputStream(bis);
      
      String s2 = su.safeReadUTF(dis);
      
      assertEquals(s, s2);   
   }
   
   public void testSafeWriteReadUTFTest2() throws Exception
   {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      
      DataOutputStream dos = new DataOutputStream(bos);
      
      String s = "abcdefghijklmnopqrstuvwxyz";
      
      SafeUTF su = new SafeUTF(30);
      
      su.safeWriteUTF(dos, s);
      
      dos.close();
      
      byte[] bytes = bos.toByteArray();
      
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      
      DataInputStream dis = new DataInputStream(bis);
      
      String s2 = su.safeReadUTF(dis);
      
      assertEquals(s, s2);
      
      int lastReadBufferSize = su.getLastReadBufferSize();
      
      assertEquals(28, lastReadBufferSize);
      
   }
   
   
   
   protected String genString(int len)
   {
      char[] chars = new char[len];
      for (int i = 0; i < len; i++)
      {
         chars[i] = (char)(65 + i % 26);
      }
      return new String(chars);
   }
   
   public void test64KLimitWithTextMessage() throws Exception
   {            
      Connection conn = null;
      
      try
      {         
         conn = cf.createConnection();
   
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue);
         
         MessageConsumer cons = sess.createConsumer(queue);
         
         conn.start();                  
               
         String s1 = genString(16 * 1024);   
         
         String s2 = genString(32 * 1024); 
         
         String s3 = genString(64 * 1024); 
         
         String s4 = genString(10 * 64 * 1024); 
         
         TextMessage tm1 = sess.createTextMessage(s1);
         
         TextMessage tm2 = sess.createTextMessage(s2);
         
         TextMessage tm3 = sess.createTextMessage(s3);
         
         TextMessage tm4 = sess.createTextMessage(s4);
         
         prod.send(tm1);
         
         prod.send(tm2);
         
         prod.send(tm3);
         
         prod.send(tm4);
   
         TextMessage rm1 = (TextMessage)cons.receive(1000);
         
         assertNotNull(rm1);           
         
         TextMessage rm2 = (TextMessage)cons.receive(1000);
         
         assertNotNull(rm2);
         
         TextMessage rm3 = (TextMessage)cons.receive(1000);
         
         assertNotNull(rm3);
         
         TextMessage rm4 = (TextMessage)cons.receive(1000);
         
         assertNotNull(rm4);
         
         assertEquals(s1.length(), rm1.getText().length());
         
         assertEquals(s1, rm1.getText());
         
         assertEquals(s2.length(), rm2.getText().length());
         
         assertEquals(s2, rm2.getText());
         
         assertEquals(s3.length(), rm3.getText().length());
         
         assertEquals(s3, rm3.getText());
         
         assertEquals(s4.length(), rm4.getText().length());
         
         assertEquals(s4, rm4.getText());
      }
      finally
      {            
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   
   
   public void test64KLimitWithObjectMessage() throws Exception
   {            
      Connection conn = null;
      
      try
      {         
         conn = cf.createConnection();
   
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sess.createProducer(queue);
         
         MessageConsumer cons = sess.createConsumer(queue);
         
         conn.start();
               
         String s1 = genString(16 * 1024);   
         
         String s2 = genString(32 * 1024); 
         
         String s3 = genString(64 * 1024); 
         
         String s4 = genString(10 * 64 * 1024);
         
         ObjectMessage om1 = sess.createObjectMessage();
         
         om1.setObject(s1);
         
         ObjectMessage om2 = sess.createObjectMessage();
         
         om2.setObject(s2);
         
         ObjectMessage om3 = sess.createObjectMessage();
         
         om3.setObject(s3);
         
         ObjectMessage om4 = sess.createObjectMessage();
         
         om4.setObject(s4);
         
         prod.send(om1);
         
         prod.send(om2);
         
         prod.send(om3);
         
         prod.send(om4);
   
         ObjectMessage rm1 = (ObjectMessage)cons.receive(1000);
         
         assertNotNull(rm1);
         
         ObjectMessage rm2 = (ObjectMessage)cons.receive(1000);
         
         assertNotNull(rm2);
         
         ObjectMessage rm3 = (ObjectMessage)cons.receive(1000);
         
         assertNotNull(rm3);
         
         ObjectMessage rm4 = (ObjectMessage)cons.receive(1000);
         
         assertNotNull(rm4);
         
         assertEquals(s1, rm1.getObject());
         
         assertEquals(s2, rm2.getObject());
         
         assertEquals(s3, rm3.getObject());
         
         assertEquals(s4, rm4.getObject());
      }
      finally
      {            
         conn.close();
      }
   }
}
