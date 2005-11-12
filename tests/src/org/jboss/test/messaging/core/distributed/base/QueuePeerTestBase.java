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
package org.jboss.test.messaging.core.distributed.base;


import org.jboss.test.messaging.core.SimpleDeliveryObserver;
import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.message.Factory;

import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class QueuePeerTestBase extends ChannelPeerTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public QueuePeerTestBase(String name)
   {
      super(name);
   }


   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   // TODO: reenable this

//   public void testSimpleSend() throws Exception
//   {
//      SimpleDeliveryObserver observer = new SimpleDeliveryObserver();
//      SimpleReceiver r = new SimpleReceiver("ONE", SimpleReceiver.ACKING);
//      channel2.add(r);
//
//      ((Peer)channel).join();
//      channel2.join();
//
//      Delivery d =
//            channel.handle(observer, Factory.createMessage("message0", false, "payload"), null);
//
//      assertTrue(d.isDone());
//      List l = r.getMessages();
//      assertEquals(1, l.size());
//      Message m = (Message)l.get(0);
//      assertEquals("payload", m.getPayload());
//   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
