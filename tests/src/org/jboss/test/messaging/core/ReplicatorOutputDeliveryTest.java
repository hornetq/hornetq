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

import org.jboss.test.messaging.core.base.SingleReceiverDeliveryTestBase;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.distributed.replicator.ReplicatorOutputDelivery;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class ReplicatorOutputDeliveryTest extends SingleReceiverDeliveryTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected DeliveryObserver observer;

   // Constructors --------------------------------------------------

   public ReplicatorOutputDeliveryTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      observer = new SimpleDeliveryObserver();
      delivery = new ReplicatorOutputDelivery(observer, null, "replicatorOutput0", false);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      delivery = null;
      observer = null;
      super.tearDown();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
