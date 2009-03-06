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
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

/**
 * A MessageWithReadResolveTest
 * 
 * See http://jira.jboss.com/jira/browse/JBMESSAGING-442
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class MessageWithReadResolveTest extends JMSTestCase
{
   
   //  Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // TestCase overrides -------------------------------------------
      
   // Public --------------------------------------------------------
   
   public void testSendReceiveMessage() throws Exception
   {
      Connection conn = null;
      
      try
      {	      
	      conn = cf.createConnection();
	      
	      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      MessageProducer prod = sess.createProducer(queue1);
	      
	      //Make persistent to make sure message gets serialized
	      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
	      
	      MessageConsumer cons = sess.createConsumer(queue1);
	      
	      TestMessage tm = new TestMessage(123, false);
	      
	      ObjectMessage om = sess.createObjectMessage();
	      
	      om.setObject(tm);
	      
	      conn.start();
	      
	      prod.send(om);
	      
	      ObjectMessage om2 = (ObjectMessage)cons.receive(1000);
	      
	      assertNotNull(om2);
	      
	      TestMessage tm2 = (TestMessage)om2.getObject();
	      
	      assertEquals(123, tm2.getID());
	      
	      conn.close();
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
            
   }
   
   /* Now test using serialization directly */
   
   /*
    * 
    * We don't currently use JBoss Serialization
   public void testUseSerializationDirectly() throws Exception
   {
      TestMessage tm = new TestMessage(456, false);
      
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      
      JBossObjectOutputStream oos = new JBossObjectOutputStream(os);
      
      oos.writeObject(tm);
      
      oos.close();
      
      byte[] bytes = os.toByteArray();
      
      ByteArrayInputStream is = new ByteArrayInputStream(bytes);
      
      JBossObjectInputStream ois = new JBossObjectInputStream(is);
      
      TestMessage tm2 = (TestMessage)ois.readObject();
      
      assertEquals(tm.id, tm2.id);
      
      ois.close();
            
   }
   */
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   /* This class would trigger the exception when serialized with jboss serialization */
   public static class TestMessage implements Serializable
   {
      private static final long serialVersionUID = -5932581134414145967L;
      private long id;
      private Object clazz;

      public TestMessage(long id, boolean useSimpleObject)
      {
        this.id = id;
        if (useSimpleObject)
        {
          clazz = String.class;
        }
        else
        {
          clazz = TestEnum.class;
        }
      }

      public String toString() 
      {
        StringBuffer sb = new StringBuffer();
        sb.append("TestMessage(");
        sb.append("id=" + id);
        sb.append(", clazz=" + clazz);
        sb.append(")");
        return sb.toString();
      }
      
      public long getID()
      {
         return id;
      }

    }
   
   public static class TestEnum implements Serializable
   {

      private static final long serialVersionUID = 4306026990380393029L;

      public Object readResolve()
      {
        return null;
      }
    }
   
   // Inner classes -------------------------------------------------
}

