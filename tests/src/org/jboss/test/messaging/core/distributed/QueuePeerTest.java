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

import org.jboss.messaging.core.message.InMemoryMessageStore;
import org.jboss.messaging.core.distributed.DistributedQueue;
import org.jboss.messaging.core.MessageStore;
import org.jboss.test.messaging.core.distributed.base.PeerTestBase;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueuePeerTest extends PeerTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected MessageStore ms;
   protected DistributedQueue queue, queue2, queue3;

   // Constructors --------------------------------------------------

   public QueuePeerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ms = new InMemoryMessageStore("persistent-message-store");
      queue = new DistributedQueue("test", ms, dispatcher);
      queue2 = new DistributedQueue("test", ms, dispatcher2);
      queue3 = new DistributedQueue("test", ms, dispatcher3);

      peer = queue.getPeer();
      peer2 = queue2.getPeer();
      peer3 = queue3.getPeer();

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      queue.leave();
      queue.close();
      queue = null;

      queue2.leave();
      queue2.close();
      queue2 = null;

      queue3.leave();
      queue3.close();
      queue3 = null;

      ms = null;

      super.tearDown();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
