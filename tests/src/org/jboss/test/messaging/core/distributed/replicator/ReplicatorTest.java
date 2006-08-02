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

import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.replicator.Replicator;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.test.messaging.core.distributed.replicator.base.ReplicatorTestBase;
import org.jgroups.blocks.RpcDispatcher;

/**
 * Test a replicator that doesn't cancel an active delivery on message rejection.
 *
 * TODO: if I need a replicator that does cancel delivery on message rejection, add a
 *       Replicator2Test.java which creates
 *       new Replicator(new SimpleViewKeeper(name), d, ms, <b>true</b>);
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ReplicatorTest extends ReplicatorTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicatorTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected Distributed createDistributed(String name, MessageStore ms, RpcDispatcher d)
   {
      // replicator doesn't cancel an active delivery on message rejection.
      return new Replicator(name, d, ms, false);
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
