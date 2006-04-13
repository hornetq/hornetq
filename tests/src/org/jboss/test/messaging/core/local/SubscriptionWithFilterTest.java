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
package org.jboss.test.messaging.core.local;

import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.local.CoreSubscription;
import org.jboss.messaging.core.message.CoreMessage;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

/**
 * 
 * A SubscriptionWithFilterTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SubscriptionWithFilterTest extends MessagingTestCase
{
   public SubscriptionWithFilterTest(String name)
   {
      super(name);
   }

   protected PersistenceManager pm;
   
   protected MessageStore ms;
   
   protected ServiceContainer sc;
    
   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      
      sc = new ServiceContainer("all,-remoting,-security");
      sc.start();
      
      pm = new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());
      pm.start();
      
      ms = new SimpleMessageStore("123");
      
      
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      pm.stop();
      
      sc.stop();

      super.tearDown();
   }
   

   public void testWithFilter()
   {
      Filter f = new SimpleFilter(3);
      
      CoreSubscription sub = new CoreSubscription(123, null, ms, pm, false, 100, 20, 10, f);
            
      Message m1 = new CoreMessage(1, false, 0, 0, (byte)0, null, null, 0);
      Message m2 = new CoreMessage(2, false, 0, 0, (byte)0, null, null, 0);
      Message m3 = new CoreMessage(3, false, 0, 0, (byte)0, null, null, 0);
      Message m4 = new CoreMessage(4, false, 0, 0, (byte)0, null, null, 0);
      Message m5 = new CoreMessage(5, false, 0, 0, (byte)0, null, null, 0);
      
      assertNull(sub.handle(null, m1, null));
      assertNull(sub.handle(null, m2, null));
      assertNotNull(sub.handle(null, m3, null));
      assertNull(sub.handle(null, m4, null));
      assertNull(sub.handle(null, m5, null));
      
   }
   
   public void testWithoutFilter()
   {
      CoreSubscription sub = new CoreSubscription(123, null, ms, pm, false, 100, 20, 10, null);
            
      Message m1 = new CoreMessage(1, false, 0, 0, (byte)0, null, null, 0);
      Message m2 = new CoreMessage(2, false, 0, 0, (byte)0, null, null, 0);
      Message m3 = new CoreMessage(3, false, 0, 0, (byte)0, null, null, 0);
      Message m4 = new CoreMessage(4, false, 0, 0, (byte)0, null, null, 0);
      Message m5 = new CoreMessage(5, false, 0, 0, (byte)0, null, null, 0);
      
      assertNotNull(sub.handle(null, m1, null));
      assertNotNull(sub.handle(null, m2, null));
      assertNotNull(sub.handle(null, m3, null));
      assertNotNull(sub.handle(null, m4, null));
      assertNotNull(sub.handle(null, m5, null));
      
   }
   
   public void crashChannel() throws Exception
   {
   }

   public void recoverChannel() throws Exception
   {
   }
   
   class SimpleFilter implements Filter
   {
      private long value;
      SimpleFilter(long value)
      {
         this.value = value;
      }
      public boolean accept(Routable routable)
      {
         CoreMessage msg = (CoreMessage)routable;
         return msg.getMessageID() == value;
      }
   }
}
