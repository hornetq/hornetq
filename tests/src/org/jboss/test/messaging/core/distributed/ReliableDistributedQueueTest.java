/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.distributed;

import org.jboss.messaging.core.persistence.HSQLDBPersistenceManager;
import org.jboss.messaging.core.message.PersistentMessageStore;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.distributed.DistributedQueue;
import org.jboss.test.messaging.core.distributed.base.DistributedQueueTestBase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ReliableDistributedQueueTest extends DistributedQueueTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private HSQLDBPersistenceManager pm;

   // Constructors --------------------------------------------------

    public ReliableDistributedQueueTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      pm = new HSQLDBPersistenceManager();
      ms = new PersistentMessageStore("persistent-message-store", pm, tm);

      channel = new DistributedQueue("test", ms, pm, tm, dispatcher);
      channelTwo = new DistributedQueue("test", msTwo, pm, tm, dispatcherTwo);
   }

   public void tearDown() throws Exception
   {
      channel.close();
      channel = null;

      channelTwo.close();
      channelTwo = null;

      pm.stop();
      ms = null;

      super.tearDown();
   }

   public void crashChannel() throws Exception
   {
      channel.close();
      channel = null;

   }

   public void recoverChannel() throws Exception
   {
      channel = new Queue("test", ms, pm, tm);
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
