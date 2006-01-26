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
import org.jboss.messaging.core.distributed.replicator.ReplicatorOutput;
import org.jboss.test.messaging.core.distributed.base.PeerTestBase;
import org.jboss.test.messaging.core.distributed.SimpleViewKeeper;
import org.jboss.jms.server.plugin.contract.MessageStore;
import org.jgroups.blocks.RpcDispatcher;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ReplicatorOutputTest extends PeerTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected SimpleViewKeeper viewKeeper;

   protected ReplicatorOutput replicatorOutput, replicatorOutput2, replicatorOutput3;

   // Constructors --------------------------------------------------

   public ReplicatorOutputTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      viewKeeper = new SimpleViewKeeper("replicator0");
      super.setUp();

      replicatorOutput = (ReplicatorOutput)distributed;
      replicatorOutput2 = (ReplicatorOutput)distributed2;
      replicatorOutput3 = (ReplicatorOutput)distributed3;

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      replicatorOutput = null;
      replicatorOutput2 = null;
      replicatorOutput3 = null;
      viewKeeper.clear();
      viewKeeper = null;
      super.tearDown();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected Distributed createDistributed(String name, MessageStore ms, RpcDispatcher d)
   {
      return new ReplicatorOutput(name, d, ms, null);
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
