/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.distributed;

import org.jboss.test.messaging.core.distributed.base.DistributedQueueTestBase;
import org.jboss.messaging.core.distributed.DistributedQueue;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class UnreliableDistributedQueueTest extends DistributedQueueTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------


   // Constructors --------------------------------------------------

   public UnreliableDistributedQueueTest(String name)
   {
      super(name);
   }

   // DistributedQueueTestBase overrides ---------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      channel = new DistributedQueue("test", ms, dispatcher);
      channelTwo = new DistributedQueue("test", msTwo, dispatcherTwo);

   }

   public void tearDown() throws Exception
   {
      channel.close();
      channel = null;

      channelTwo.close();
      channelTwo = null;

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
