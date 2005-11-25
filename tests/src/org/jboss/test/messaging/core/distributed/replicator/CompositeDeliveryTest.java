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
package org.jboss.test.messaging.core.distributed.replicator;

import org.jboss.test.messaging.core.SimpleReceiver;
import org.jboss.test.messaging.core.distributed.replicator.base.MultipleReceiversDeliveryTestBase;
import org.jboss.messaging.core.distributed.replicator.CompositeDelivery;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jgroups.stack.IpAddress;

import java.io.Serializable;

/**
 * Test a composite delivery that gets cancelled when receives a message rejection.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class CompositeDeliveryTest extends MultipleReceiversDeliveryTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public CompositeDeliveryTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      delivery = new CompositeDelivery(observer, ref, true);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      delivery = null;
      super.tearDown();
   }

   // Public --------------------------------------------------------

   public void testArgument()
   {
      SimpleReceiver r = new SimpleReceiver();
      try
      {
         ((CompositeDelivery)delivery).add(r);
         fail("should throw IllegalArgumentException");
      }
      catch(IllegalArgumentException e)
      {
         // OK
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected Object createReceiver(Serializable replicatorID, Serializable outputID)
   {
      return new PeerIdentity(replicatorID, outputID, new IpAddress("localhost", 7777));
   }

   protected void assertEqualsReceiver(Serializable outputID, Object receiver)
   {
      assertEquals(outputID, ((PeerIdentity)receiver).getPeerID());
   }


   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
