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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

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
public class String64KLimitTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public String64KLimitTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------
         
   protected String genString(int len)
   {
      char[] chars = new char[len];
      for (int i = 0; i < len; i++)
      {
         chars[i] = (char)(65 + i % 26);
      }
      return new String(chars);
   }
   
   //Tests commented out until message chunking is complete
   //See http://jira.jboss.org/jira/browse/JBMESSAGING-379

   public void testFoo() throws Exception
   {      
   }
   
//   public void test64KLimitWithTextMessage() throws Exception
//   {            
//      Connection conn = null;
//      
//      try
//      {         
//         conn = cf.createConnection();
//   
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         
//         MessageProducer prod = sess.createProducer(queue1);
//         
//         MessageConsumer cons = sess.createConsumer(queue1);
//         
//         conn.start();                  
//               
//         String s1 = genString(16 * 1024);   
//         
//         String s2 = genString(32 * 1024); 
//         
//         String s3 = genString(64 * 1024); 
//         
//         String s4 = genString(10 * 64 * 1024); 
//         
//         TextMessage tm1 = sess.createTextMessage(s1);
//         
//         TextMessage tm2 = sess.createTextMessage(s2);
//         
//         TextMessage tm3 = sess.createTextMessage(s3);
//         
//         TextMessage tm4 = sess.createTextMessage(s4);
//         
//         prod.send(tm1);
//         
//         prod.send(tm2);
//         
//         prod.send(tm3);
//         
//         prod.send(tm4);
//   
//         TextMessage rm1 = (TextMessage)cons.receive(1000);
//         
//         assertNotNull(rm1);           
//         
//         TextMessage rm2 = (TextMessage)cons.receive(1000);
//         
//         assertNotNull(rm2);
//         
//         TextMessage rm3 = (TextMessage)cons.receive(1000);
//         
//         assertNotNull(rm3);
//         
//         TextMessage rm4 = (TextMessage)cons.receive(1000);
//         
//         assertNotNull(rm4);
//         
//         assertEquals(s1.length(), rm1.getText().length());
//         
//         assertEquals(s1, rm1.getText());
//         
//         assertEquals(s2.length(), rm2.getText().length());
//         
//         assertEquals(s2, rm2.getText());
//         
//         assertEquals(s3.length(), rm3.getText().length());
//         
//         assertEquals(s3, rm3.getText());
//         
//         assertEquals(s4.length(), rm4.getText().length());
//         
//         assertEquals(s4, rm4.getText());
//      }
//      finally
//      {            
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//   }
//         
//   public void test64KLimitWithObjectMessage() throws Exception
//   {            
//      Connection conn = null;
//      
//      try
//      {         
//         conn = cf.createConnection();
//   
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         
//         MessageProducer prod = sess.createProducer(queue1);
//         
//         MessageConsumer cons = sess.createConsumer(queue1);
//         
//         conn.start();
//               
//         String s1 = genString(16 * 1024);   
//         
//         String s2 = genString(32 * 1024); 
//         
//         String s3 = genString(64 * 1024); 
//         
//         String s4 = genString(10 * 64 * 1024);
//         
//         ObjectMessage om1 = sess.createObjectMessage();
//         
//         om1.setObject(s1);
//         
//         ObjectMessage om2 = sess.createObjectMessage();
//         
//         om2.setObject(s2);
//         
//         ObjectMessage om3 = sess.createObjectMessage();
//         
//         om3.setObject(s3);
//         
//         ObjectMessage om4 = sess.createObjectMessage();
//         
//         om4.setObject(s4);
//         
//         prod.send(om1);
//         
//         prod.send(om2);
//         
//         prod.send(om3);
//         
//         prod.send(om4);
//   
//         ObjectMessage rm1 = (ObjectMessage)cons.receive(1000);
//         
//         assertNotNull(rm1);
//         
//         ObjectMessage rm2 = (ObjectMessage)cons.receive(1000);
//         
//         assertNotNull(rm2);
//         
//         ObjectMessage rm3 = (ObjectMessage)cons.receive(1000);
//         
//         assertNotNull(rm3);
//         
//         ObjectMessage rm4 = (ObjectMessage)cons.receive(1000);
//         
//         assertNotNull(rm4);
//         
//         assertEquals(s1, rm1.getObject());
//         
//         assertEquals(s2, rm2.getObject());
//         
//         assertEquals(s3, rm3.getObject());
//         
//         assertEquals(s4, rm4.getObject());
//      }
//      finally
//      {            
//         conn.close();
//      }
//   }
}
