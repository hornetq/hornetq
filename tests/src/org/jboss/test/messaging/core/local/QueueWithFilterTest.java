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
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.message.CoreMessage;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A QueueWithFilterTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision: 1237 $
 *
 * $Id: queueWithFilterTest.java 1237 2006-08-30 18:36:36Z timfox $
 */
public class QueueWithFilterTest extends MessagingTestCase
{
   public QueueWithFilterTest(String name)
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
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
      
      ms = new SimpleMessageStore();
      ms.start();
            
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      pm.stop();
      
      ms.stop();
      
      sc.stop();
      
      super.tearDown();
   }
   

   public void testWithFilter()
   {
      Filter f = new SimpleFilter(3);
      
      Queue queue = new Queue(1, ms, pm, true, true, 100, 20, 10, new QueuedExecutor(),
                                                   f);
       
      Message m1 = new CoreMessage(1, false, 0, 0, (byte)0, null, null, 0);
      Message m2 = new CoreMessage(2, false, 0, 0, (byte)0, null, null, 0);
      Message m3 = new CoreMessage(3, false, 0, 0, (byte)0, null, null, 0);
      Message m4 = new CoreMessage(4, false, 0, 0, (byte)0, null, null, 0);
      Message m5 = new CoreMessage(5, false, 0, 0, (byte)0, null, null, 0);
      
      assertNull(queue.handle(null, ms.reference(m1), null));
      assertNull(queue.handle(null, ms.reference(m2), null));
      assertNotNull(queue.handle(null, ms.reference(m3), null));
      assertNull(queue.handle(null, ms.reference(m4), null));
      assertNull(queue.handle(null, ms.reference(m5), null));
      
   }
   
   public void testWithoutFilter()
   {
      Queue queue = new Queue(1, ms, pm, true, true, 100, 20, 10, new QueuedExecutor(),
                                                   null);     
      
      Message m1 = new CoreMessage(1, false, 0, 0, (byte)0, null, null, 0);
      Message m2 = new CoreMessage(2, false, 0, 0, (byte)0, null, null, 0);
      Message m3 = new CoreMessage(3, false, 0, 0, (byte)0, null, null, 0);
      Message m4 = new CoreMessage(4, false, 0, 0, (byte)0, null, null, 0);
      Message m5 = new CoreMessage(5, false, 0, 0, (byte)0, null, null, 0);
      
      assertNotNull(queue.handle(null, ms.reference(m1), null));
      assertNotNull(queue.handle(null, ms.reference(m2), null));
      assertNotNull(queue.handle(null, ms.reference(m3), null));
      assertNotNull(queue.handle(null, ms.reference(m4), null));
      assertNotNull(queue.handle(null, ms.reference(m5), null));
      
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
      public String getFilterString()
      {
         return null;
      }
   }
}
