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

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jgroups.stack.IpAddress;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PeerIdentityTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public PeerIdentityTest(String name)
   {
      super(name);
   }

   // Protected -----------------------------------------------------

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testEquals1() throws Exception
   {
      PeerIdentity id1 =
            new PeerIdentity("distributedID", "peerID", new IpAddress("localhost", 777));
      PeerIdentity id2 =
            new PeerIdentity("distributedID", "peerID", new IpAddress("localhost", 777));

      assertEquals(id1, id2);
   }

   public void testEquals2() throws Exception
   {
      PeerIdentity id1 = new PeerIdentity(null, null, null);
      PeerIdentity id2 = new PeerIdentity(null, null, null);

      assertEquals(id1, id2);
   }

   public void testEquals3() throws Exception
   {
      PeerIdentity id1 = new PeerIdentity("distributedID", null, null);
      PeerIdentity id2 = new PeerIdentity("distributedID", null, null);

      assertEquals(id1, id2);
   }


}
