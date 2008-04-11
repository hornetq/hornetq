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
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.transaction.Transaction;
import javax.transaction.UserTransaction;

import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.tm.TransactionManagerLocator;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * $Id: JCAWrapperTest.java 1019 2006-07-17 17:15:04Z timfox $
 */
public class JCAWrapperTest extends JMSTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------
   
   public JCAWrapperTest(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------

   public void testSimpleTransactedSend() throws Exception
   {
      Transaction suspended = TransactionManagerLocator.getInstance().locate().suspend();
      
      Connection conn = null;
      
      try
      {
         
         ConnectionFactory mcf = (ConnectionFactory)ic.lookup("java:/JCAConnectionFactory");
         conn = mcf.createConnection();
         conn.start();
   
         UserTransaction ut = ServerManagement.getUserTransaction();
   
         ut.begin();
   
         Session s = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer p = s.createProducer(queue1);
         Message m = s.createTextMessage("one");
   
         p.send(m);
   
         ut.commit();
   
         conn.close();
   
         ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
         conn = cf.createConnection();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn.start();
   
         TextMessage rm = (TextMessage)s.createConsumer(queue1).receive(500);
   
         assertEquals("one", rm.getText());

         conn.close();
         
         log.info("**** class is " + mcf);                  
      }
      finally
      {      
         if (conn != null)
         {
         	conn.close();
         }
         
         if (suspended != null)
         {
            TransactionManagerLocator.getInstance().locate().resume(suspended);
         }                  
      }
   }

   /**
    * The difference from the previous test is that we're creating the session using
    * conn.createSession(false, ...);
    */
   public void testSimpleTransactedSend2() throws Exception
   {
      Transaction suspended = TransactionManagerLocator.getInstance().locate().suspend();
      
      Connection conn = null;

      try
      {
         ConnectionFactory mcf = (ConnectionFactory)ic.lookup("java:/JCAConnectionFactory");
         conn = mcf.createConnection();
         conn.start();

         UserTransaction ut = ServerManagement.getUserTransaction();

         ut.begin();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue1);
         Message m = s.createTextMessage("one");

         p.send(m);

         ut.commit();

         conn.close();

         ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");
         conn = cf.createConnection();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn.start();

         TextMessage rm = (TextMessage)s.createConsumer(queue1).receive(500);

         assertEquals("one", rm.getText());
      }
      finally
      {         
         if (conn != null)
         {
         	conn.close();
         }

         if (suspended != null)
         {
            TransactionManagerLocator.getInstance().locate().resume(suspended);
         }
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void tearDown() throws Exception
   {
   	//We don't want the managed connection pool hanging on to connections
      ServerManagement.getServer(0).flushManagedConnectionPool();
      
   	super.tearDown();   	
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}


