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
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.plugin.IdManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.core.tx.TransactionRepository;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class SimpleDeliveryTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected DeliveryObserver observer;
   
   protected Delivery delivery;

   // Constructors --------------------------------------------------

   public SimpleDeliveryTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      observer = new SimpleDeliveryObserver();
      delivery = new SimpleDelivery(observer, null, false);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      delivery = null;
      observer = null;
      super.tearDown();
   }

   // Public --------------------------------------------------------
   
   public void testAcknowledgment() throws Throwable
   {
      assertFalse(delivery.isDone());

      delivery.acknowledge(null);

      assertTrue(delivery.isDone());
   }
   
   public void testCancellation() throws Throwable
   {
      assertFalse(delivery.isCancelled());

      delivery.cancel();

      assertTrue(delivery.isCancelled());
   }
   
   public void testDoneIsSetWithTransaction() throws Throwable
   {
      //Calling acknowledge on a SimpleDelivery
      //Should always result in done being set to true,
      //even if there is a transaction present.
      //Otherwise we can end up with a race condition where
      //the message is acked when still in flight then added
      //when handle is returned.
      
      assertFalse(delivery.isDone());
      
      ServiceContainer sc = new ServiceContainer("all,-remoting,-security");
      sc.start();
      
      PersistenceManager pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(), null,
                                    true, true, true, 100);      
      pm.start();
      
      TransactionRepository tr = new TransactionRepository(pm, new IdManager("TRANSACTION_ID", 10, pm));
      tr.start();
      
      Transaction tx = tr.createTransaction();
      
      ((SimpleDelivery)delivery).acknowledge(tx);
      
      assertTrue(delivery.isDone());
      
      pm.stop();
      tr.stop();
      
      sc.stop();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
