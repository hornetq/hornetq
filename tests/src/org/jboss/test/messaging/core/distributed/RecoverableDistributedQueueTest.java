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
package org.jboss.test.messaging.core.distributed;

import org.jboss.messaging.core.persistence.JDBCPersistenceManager;
import org.jboss.messaging.core.message.PersistentMessageStore;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.distributed.queue.DistributedQueue;
import org.jboss.messaging.core.distributed.queue.DistributedQueue;
import org.jboss.test.messaging.core.distributed.base.DistributedQueueTestBase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class RecoverableDistributedQueueTest extends DistributedQueueTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private JDBCPersistenceManager pm;
   private JDBCPersistenceManager pm2;
   private JDBCPersistenceManager pm3;

   // Constructors --------------------------------------------------

    public RecoverableDistributedQueueTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      pm = new JDBCPersistenceManager();
      pm2 = new JDBCPersistenceManager();
      pm3 = new JDBCPersistenceManager();

      ms = new PersistentMessageStore("persistent-message-store", pm);
      ms2 = new PersistentMessageStore("persistent-message-store2", pm2);
      ms3 = new PersistentMessageStore("persistent-message-store3", pm3);

      channel = new DistributedQueue("test", ms, pm, dispatcher);
      channel2 = new DistributedQueue("test", ms2, pm2, dispatcher2);
      channel3 = new DistributedQueue("test", ms3, pm3, dispatcher3);

      tr.setPersistenceManager(pm);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      channel.close();
      channel = null;

      channel2.close();
      channel2 = null;

      channel3.close();
      channel3 = null;

      pm.stop();
      ms = null;

      pm2.stop();
      ms2 = null;

      pm3.stop();
      ms3 = null;

      super.tearDown();
   }

   public void crashChannel() throws Exception
   {
      channel.close();
      channel = null;

   }

   public void recoverChannel() throws Exception
   {
      channel = new Queue("test", ms, pm);
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
