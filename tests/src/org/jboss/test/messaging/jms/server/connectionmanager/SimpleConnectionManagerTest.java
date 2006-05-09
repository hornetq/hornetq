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
package org.jboss.test.messaging.jms.server.connectionmanager;

import javax.jms.JMSException;
import javax.naming.InitialContext;
import javax.transaction.xa.Xid;

import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.server.connectionmanager.SimpleConnectionManager;
import org.jboss.jms.server.endpoint.ConnectionEndpoint;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.messaging.core.plugin.IdBlock;

/**
 * 
 * A SimpleConnectionManagerTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * SimpleConnectionManagerTest.java,v 1.1 2006/04/14 12:27:06 timfox Exp
 */
public class SimpleConnectionManagerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public SimpleConnectionManagerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("this test is not supposed to run in a remote configuration!");
      }

      super.setUp();
      ServerManagement.start("all");

      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();

      initialContext.close();
   }

   public void testSimpleConnectionManager() throws Exception
   {
      SimpleConnectionManager cm = new SimpleConnectionManager();

      SimpleConnectionEndpoint e1 = new SimpleConnectionEndpoint();
      SimpleConnectionEndpoint e2 = new SimpleConnectionEndpoint();
      SimpleConnectionEndpoint e3 = new SimpleConnectionEndpoint();
      SimpleConnectionEndpoint e4 = new SimpleConnectionEndpoint();
      SimpleConnectionEndpoint e5 = new SimpleConnectionEndpoint();
      SimpleConnectionEndpoint e6 = new SimpleConnectionEndpoint();

      assertFalse(e1.isClosed());
      assertFalse(e2.isClosed());
      assertFalse(e3.isClosed());
      assertFalse(e4.isClosed());
      assertFalse(e5.isClosed());
      assertFalse(e6.isClosed());

      cm.registerConnection("jvm1", "sessionid1", e1);
      cm.registerConnection("jvm1", "sessionid2", e2);
      cm.registerConnection("jvm2", "sessionid3", e3);
      cm.registerConnection("jvm2", "sessionid4", e4);
      cm.registerConnection("jvm3", "sessionid5", e5);
      cm.registerConnection("jvm3", "sessionid6", e6);

      assertTrue(cm.containsSession("sessionid1"));
      assertTrue(cm.containsSession("sessionid2"));
      assertTrue(cm.containsSession("sessionid3"));
      assertTrue(cm.containsSession("sessionid4"));
      assertTrue(cm.containsSession("sessionid5"));
      assertTrue(cm.containsSession("sessionid6"));

      ConnectionEndpoint r1 = cm.unregisterConnection("jvm3", "sessionid6");
      assertEquals(e6, r1);
      assertFalse(e6.isClosed());

      assertNull(cm.unregisterConnection("blah", "blah"));

      assertFalse(cm.containsSession("sessionid6"));

      ConnectionEndpoint r2 = cm.unregisterConnection("jvm3", "sessionid5");
      assertEquals(e5, r2);
      assertFalse(e5.isClosed());

      assertFalse(cm.containsSession("sessionid5"));

      cm.handleClientFailure("sessionid4");

      assertNull(cm.unregisterConnection("jvm2", "sessionid4"));
      assertNull(cm.unregisterConnection("jvm2", "sessionid3"));

      assertFalse(cm.containsSession("sessionid4"));
      assertFalse(cm.containsSession("sessionid3"));

      assertTrue(e3.isClosed());
      assertTrue(e4.isClosed());

      ConnectionEndpoint r3 = cm.unregisterConnection("jvm1", "sessionid1");
      assertEquals(e1, r3);
      assertFalse(e1.isClosed());

      ConnectionEndpoint r4 = cm.unregisterConnection("jvm1", "sessionid2");
      assertEquals(e2, r4);
      assertFalse(e2.isClosed());

      assertFalse(cm.containsSession("sessionid2"));
      assertFalse(cm.containsSession("sessionid1"));

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class SimpleConnectionEndpoint implements ConnectionEndpoint
   {
      public boolean closed;

      SimpleConnectionEndpoint()
      {
         closed = false;
      }

      public boolean isClosed()
      {
         return closed;
      }

      public SessionDelegate createSessionDelegate(boolean transacted, int acknowledgmentMode, boolean isXA) throws JMSException
      {
         return null;
      }

      public String getClientID() throws JMSException
      {
         return null;
      }

      public Xid[] getPreparedTransactions()
      {
         return null;
      }

      public void sendTransaction(TransactionRequest request) throws JMSException
      {
      }

      public void setClientID(String id) throws JMSException
      {
      }

      public void start() throws JMSException
      {
      }

      public void stop() throws JMSException
      {
      }

      public void close() throws JMSException
      {
         closed = true;
      }

      public void closing() throws JMSException
      {
      }

      public IdBlock getIDBlock(int size) throws JMSException
      {
         return null;
      }
   }
}

