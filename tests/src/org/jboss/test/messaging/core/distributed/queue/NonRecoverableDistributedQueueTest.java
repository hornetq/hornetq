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
package org.jboss.test.messaging.core.distributed.queue;

import org.jboss.messaging.core.distributed.queue.DistributedQueue;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.SimpleMessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.test.messaging.core.distributed.queue.base.DistributedQueueTestBase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class NonRecoverableDistributedQueueTest extends DistributedQueueTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public NonRecoverableDistributedQueueTest(String name)
   {
      super(name);
   }

   // DistributedQueueTestBase overrides ---------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      channel = new DistributedQueue("test", ms, dispatcher);
      
      PersistenceManager pm = new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager());
      pm.start();

      ms2 = new SimpleMessageStore("s45");

      ms3 = new SimpleMessageStore("s46");

      channel2 = new DistributedQueue("test", ms2, dispatcher2);
      channel3 = new DistributedQueue("test", ms3, dispatcher3);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      channel.close();
      channel = null;

      ms2 = null;
      channel2.close();
      channel2 = null;

      ms3 = null;
      channel3.close();
      channel3 = null;
      
      super.tearDown();
   }

   public void crashChannel() throws Exception
   {
      // doesn't matter
   }

   public void recoverChannel() throws Exception
   {
      // doesn't matter
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
