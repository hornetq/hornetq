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
import org.jboss.messaging.core.distributed.DistributedChannel;
import org.jboss.messaging.core.MessageStore;
import org.jboss.test.messaging.core.distributed.base.PeerTestBase;
import org.jgroups.blocks.RpcDispatcher;

import java.io.Serializable;

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

      channel = createDistributedChannel("test", dispatcher);
      channel2 = createDistributedChannel("test", dispatcher2);
      channel3 = createDistributedChannel("test", dispatcher3);

      peer = channel.getPeer();
      peer2 = channel2.getPeer();
      peer3 = channel3.getPeer();

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

      ms = null;

      super.tearDown();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected DistributedChannel createDistributedChannel(Serializable channelID, RpcDispatcher d)
   {
      return new DistributedQueue((String)channelID, ms, d);
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
