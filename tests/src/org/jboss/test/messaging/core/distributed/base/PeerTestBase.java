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


import org.jboss.test.messaging.core.distributed.JGroupsUtil;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;

import java.util.Set;

/**
 * The test strategy is to group at this level all peer-related tests. It assumes two distinct
 * JGroups JChannel instances and two channel peers (channel and channel2)
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class PeerTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected JChannel jchannel, jchannel2, jchannel3;
   protected RpcDispatcher dispatcher, dispatcher2, dispatcher3;

   protected Peer peer, peer2, peer3;

   // Constructors --------------------------------------------------

   public PeerTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      jchannel = new JChannel(JGroupsUtil.generateProperties(50, 1));
      jchannel2 = new JChannel(JGroupsUtil.generateProperties(900000, 1));
      jchannel3 = new JChannel(JGroupsUtil.generateProperties(900000, 2));

      dispatcher = new RpcDispatcher(jchannel, null, null, new RpcServer("1"));
      dispatcher2 = new RpcDispatcher(jchannel2, null, null, new RpcServer("2"));
      dispatcher3 = new RpcDispatcher(jchannel3, null, null, new RpcServer("3"));

      // connect only the first JChannel
      jchannel.connect("testGroup");

      assertEquals(1, jchannel.getView().getMembers().size());
   }

   public void tearDown() throws Exception
   {
      jchannel.close();
      jchannel2.close();
      jchannel3.close();

      super.tearDown();
   }

   public void testJGroupsChannelNotConnected() throws Exception
   {
      assertTrue(jchannel.isConnected());
      jchannel.close();

      try
      {
         peer.join();
         fail("should throw DistributedException");
      }
      catch(DistributedException e)
      {
         //OK
      }
   }


   public void testPeerInGroupOfOne() throws Exception
   {
      assertTrue(jchannel.isConnected());

      PeerIdentity peerIdentity = peer.getPeerIdentity();
      assertEquals(peer.getGroupID(), peerIdentity.getGroupID());

      peer.join();

      assertTrue(peer.hasJoined());

      assertEquals(peerIdentity, peer.getPeerIdentity());

      Set view = peer.getView();

      assertEquals(1, view.size());
      assertTrue(view.contains(peerIdentity));

      peer.leave();

      assertFalse(peer.hasJoined());

      assertEquals(peerIdentity, peer.getPeerIdentity());

      view = peer.getView();
      assertEquals(0, view.size());
   }

   public void testPeerInGroupOfTwo() throws Exception
   {
      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      PeerIdentity peerIdentity = peer.getPeerIdentity();
      PeerIdentity peer2Identity = peer2.getPeerIdentity();

      assertEquals(peer.getGroupID(), peerIdentity.getGroupID());
      assertEquals(peer.getGroupID(), peer2Identity.getGroupID());
      assertFalse(peerIdentity.getPeerID().equals(peer2Identity.getPeerID()));

      peer.join();
      log.debug("peer has joined");

      assertTrue(peer.hasJoined());

      Set view = peer.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peerIdentity));

      peer2.join();
      log.debug("peer2 has joined");

      assertTrue(peer2.hasJoined());

      view = peer.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      peer.leave();
      log.debug("peer has left");

      assertFalse(peer.hasJoined());

      view = peer.getView();
      assertEquals(0, view.size());
      view = peer2.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peer2Identity));

      peer2.leave();
      log.debug("peer2 has left");

      assertFalse(peer2.hasJoined());

      view = peer.getView();
      assertEquals(0, view.size());
      view = peer2.getView();
      assertEquals(0, view.size());
   }

   public void testPeerInGroupOfThree() throws Exception
   {
      jchannel2.connect("testGroup");
      jchannel3.connect("testGroup");

      // allow the group time to form
      Thread.sleep(2000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());
      assertTrue(jchannel3.isConnected());

      // make sure all three jchannels joined the group
      assertEquals(3, jchannel.getView().getMembers().size());
      assertEquals(3, jchannel2.getView().getMembers().size());
      assertEquals(3, jchannel3.getView().getMembers().size());

      PeerIdentity peerIdentity = peer.getPeerIdentity();
      PeerIdentity peer2Identity = peer2.getPeerIdentity();
      PeerIdentity peer3Identity = peer3.getPeerIdentity();

      assertEquals(peer.getGroupID(), peerIdentity.getGroupID());
      assertEquals(peer.getGroupID(), peer2Identity.getGroupID());
      assertEquals(peer.getGroupID(), peer3Identity.getGroupID());

      assertFalse(peerIdentity.getPeerID().equals(peer2Identity.getPeerID()));
      assertFalse(peerIdentity.getPeerID().equals(peer3Identity.getPeerID()));
      assertFalse(peer2Identity.getPeerID().equals(peer3Identity.getPeerID()));

      peer.join();
      log.debug("peer has joined");

      assertTrue(peer.hasJoined());

      Set view = peer.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peerIdentity));

      peer2.join();
      log.debug("peer2 has joined");

      assertTrue(peer2.hasJoined());

      view = peer.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      peer3.join();
      log.debug("peer3 has joined");

      assertTrue(peer3.hasJoined());

      view = peer.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      view = peer2.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      view = peer3.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      peer.leave();
      log.debug("peer has left");

      assertFalse(peer.hasJoined());

      view = peer.getView();
      assertEquals(0, view.size());

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      view = peer3.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      peer2.leave();
      log.debug("peer2 has left");

      assertFalse(peer2.hasJoined());

      view = peer2.getView();
      assertEquals(0, view.size());

      view = peer3.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peer3Identity));

      peer3.leave();
      log.debug("peer3 has left");

      assertFalse(peer3.hasJoined());

      view = peer3.getView();
      assertEquals(0, view.size());

   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
