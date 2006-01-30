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
import org.jboss.messaging.core.distributed.Distributed;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.InMemoryMessageStore;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.InMemoryMessageStore;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;

import java.util.Set;

/**
 * The test strategy is to group at this level all peer-related tests. It assumes two distinct
 * JGroups JChannel instances and two destination peers (destination and distributed2)
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

   protected Distributed distributed, distributed2, distributed3;
   protected Peer peer, peer2, peer3;

   // will be shared, but it doesn't matter, the test doesn't use it anyway
   protected MessageStore ms;

   // Constructors --------------------------------------------------

   public PeerTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ms = new InMemoryMessageStore("shared-in-memory-store");

      jchannel = new JChannel(JGroupsUtil.generateProperties(50, 1));
      jchannel2 = new JChannel(JGroupsUtil.generateProperties(900000, 1));
      jchannel3 = new JChannel(JGroupsUtil.generateProperties(900000, 2));

      dispatcher = new RpcDispatcher(jchannel, null, null, new RpcServer("1"));
      dispatcher2 = new RpcDispatcher(jchannel2, null, null, new RpcServer("2"));
      dispatcher3 = new RpcDispatcher(jchannel3, null, null, new RpcServer("3"));

      distributed = createDistributed("test", ms, dispatcher);
      distributed2 = createDistributed("test", ms, dispatcher2);
      distributed3 = createDistributed("test", ms, dispatcher3);

      peer = distributed.getPeer();
      peer2 = distributed2.getPeer();
      peer3 = distributed3.getPeer();

      // connect only the first JChannel
      jchannel.connect("testGroup");

      assertEquals(1, jchannel.getView().getMembers().size());
   }

   public void tearDown() throws Exception
   {
      distributed.close();
      distributed = null;

      distributed2.close();
      distributed2 = null;

      distributed3.close();
      distributed3 = null;
      
      jchannel.close();
      jchannel2.close();
      jchannel3.close();

      super.tearDown();
   }

   public void testNullRpcServer() throws Exception
   {
      JChannel jchannel = new JChannel(JGroupsUtil.generateProperties(50, 1));
      RpcDispatcher dispatcher = new RpcDispatcher(jchannel, null, null, null);

      try
      {
         createDistributed("test", ms, dispatcher);
         fail("should throw IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
   }

   public void testNoRpcServer() throws Exception
   {
      JChannel jchannel = new JChannel(JGroupsUtil.generateProperties(50, 1));
      RpcDispatcher dispatcher = new RpcDispatcher(jchannel, null, null, new Object());

      try
      {
         createDistributed("test", ms, dispatcher);
         fail("should throw IllegalStateException");
      }
      catch(IllegalStateException e)
      {
         // OK
      }
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

   /**
    * One peer uses a single channel and forms a singleton group.
    */
   public void testSingletonGroup() throws Exception
   {
      assertTrue(jchannel.isConnected());

      PeerIdentity peerIdentity = peer.getPeerIdentity();
      assertEquals(peer.getGroupID(), peerIdentity.getGroupID());

      peer.join();

      log.debug("peer joined");

      assertTrue(peer.hasJoined());

      assertEquals(peerIdentity, peer.getPeerIdentity());

      Set view = peer.getView();

      assertEquals(1, view.size());
      assertTrue(view.contains(peerIdentity));

      Set ping = peer.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peerIdentity));

      peer.leave();

      log.debug("peer left");

      assertFalse(peer.hasJoined());

      assertEquals(peerIdentity, peer.getPeerIdentity());

      view = peer.getView();
      assertEquals(0, view.size());

      ping = peer.ping();
      assertTrue(ping.isEmpty());

      // call leave twice
      peer.leave();
   }

   /**
    * Two peers peer use one single physical JGroups channel.
    */
   public void testTwoPeers_1() throws Exception
   {
      assertTrue(jchannel.isConnected());
      assertEquals(1, jchannel.getView().getMembers().size());

      distributed2.close();

      // create a new distributed2 in top of the *same* dispatcher
      Distributed destination2 = createDistributed((String)peer.getGroupID(), ms, dispatcher);
      peer2 = destination2.getPeer();

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

      Set ping = peer.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peerIdentity));

      log.debug("peer2 joining");
      peer2.join();
      log.debug("peer2 has joined");

      assertTrue(peer2.hasJoined());

      view = peer.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer2.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      peer.leave();
      log.debug("peer has left");

      assertFalse(peer.hasJoined());

      view = peer.getView();
      assertTrue(view.isEmpty());

      ping = peer.ping();
      assertTrue(ping.isEmpty());

      view = peer2.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peer2Identity));

      ping = peer2.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peer2Identity));

      peer2.leave();
      log.debug("peer2 has left");

      assertFalse(peer2.hasJoined());

      view = peer.getView();
      assertTrue(view.isEmpty());

      ping = peer.ping();
      assertTrue(ping.isEmpty());

      view = peer2.getView();
      assertTrue(view.isEmpty());

      ping = peer2.ping();
      assertTrue(ping.isEmpty());
   }

   /**
    * Two peers peer use two different physical JGroups channels.
    */
   public void testTwoPeers_2() throws Exception
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

      Set ping = peer.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peerIdentity));

      peer2.join();
      log.debug("peer2 has joined");

      assertTrue(peer2.hasJoined());

      view = peer.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer2.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      peer.leave();
      log.debug("peer has left");

      assertFalse(peer.hasJoined());

      view = peer.getView();
      assertEquals(0, view.size());

      ping = peer.ping();
      assertTrue(ping.isEmpty());

      view = peer2.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peer2Identity));

      ping = peer2.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peer2Identity));

      // call leave twice
      peer.leave();

      peer2.leave();
      log.debug("peer2 has left");

      assertFalse(peer2.hasJoined());

      view = peer.getView();
      assertEquals(0, view.size());

      ping = peer.ping();
      assertTrue(ping.isEmpty());

      view = peer2.getView();
      assertEquals(0, view.size());

      ping = peer2.ping();
      assertTrue(ping.isEmpty());

      // call leave twice
      peer2.leave();
   }


   /**
    * Three peers peer use one single physical JGroups channel.
    */
   public void testThreePeers_1() throws Exception
   {
      assertTrue(jchannel.isConnected());
      assertEquals(1, jchannel.getView().getMembers().size());

      distributed2.close();
      distributed3.close();

      // create a new distributed2 in top of the *same* dispatcher
      Distributed destination2 = createDistributed((String)peer.getGroupID(), ms, dispatcher);
      peer2 = destination2.getPeer();

      // create a new distributed3 in top of the *same* dispatcher
      Distributed destination3 = createDistributed((String)peer.getGroupID(), ms, dispatcher);
      peer3 = destination3.getPeer();

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

      Set ping = peer.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peerIdentity));

      peer2.join();
      log.debug("peer2 has joined");

      assertTrue(peer2.hasJoined());

      view = peer.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer2.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      peer3.join();
      log.debug("peer3 has joined");

      assertTrue(peer3.hasJoined());

      view = peer.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer2.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer2.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer3.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      peer.leave();
      log.debug("peer has left");

      assertFalse(peer.hasJoined());

      view = peer.getView();
      assertTrue(view.isEmpty());

      ping = peer.ping();
      assertTrue(view.isEmpty());

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer2.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer3.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      peer2.leave();
      log.debug("peer2 has left");

      assertFalse(peer2.hasJoined());

      view = peer2.getView();
      assertTrue(view.isEmpty());

      ping = peer2.ping();
      assertTrue(view.isEmpty());

      view = peer3.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peer3Identity));

      peer3.leave();
      log.debug("peer3 has left");

      assertFalse(peer3.hasJoined());

      view = peer3.getView();
      assertTrue(view.isEmpty());

      ping = peer3.ping();
      assertTrue(view.isEmpty());
   }

   /**
    * Three peers peer use two physical JGroups channel (two peers use the first JGroups channel and
    * the third uses the second JGroups channel.
    */
   public void testThreePeers_2() throws Exception
   {
      jchannel2.connect("testGroup");

      // allow the group time to form
      Thread.sleep(1000);

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      // make sure both jchannels joined the group
      assertEquals(2, jchannel.getView().getMembers().size());
      assertEquals(2, jchannel2.getView().getMembers().size());

      distributed3.close();

      // create a new distributed3 in top of the *same* dispatcher
      Distributed destination3 = createDistributed((String)peer.getGroupID(), ms, dispatcher);
      peer3 = destination3.getPeer();

      // the first jchannel already joined the group

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

      Set ping = peer.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peerIdentity));

      peer2.join();
      log.debug("peer2 has joined");

      assertTrue(peer2.hasJoined());

      view = peer.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer2.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      peer3.join();
      log.debug("peer3 has joined");

      assertTrue(peer3.hasJoined());

      view = peer.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer2.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer2.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer3.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      peer.leave();
      log.debug("peer has left");

      assertFalse(peer.hasJoined());

      view = peer.getView();
      assertTrue(view.isEmpty());

      ping = peer.ping();
      assertTrue(view.isEmpty());

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer2.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer3.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      peer2.leave();
      log.debug("peer2 has left");

      assertFalse(peer2.hasJoined());

      view = peer2.getView();
      assertTrue(view.isEmpty());

      ping = peer2.ping();
      assertTrue(view.isEmpty());

      view = peer3.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peer3Identity));

      peer3.leave();
      log.debug("peer3 has left");

      assertFalse(peer3.hasJoined());

      view = peer3.getView();
      assertTrue(view.isEmpty());

      ping = peer3.ping();
      assertTrue(view.isEmpty());
   }


   /**
    * Three peers peer use three physical JGroups channels.
    */
   public void testThreePeers_3() throws Exception
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

      Set ping = peer.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peerIdentity));

      peer2.join();
      log.debug("peer2 has joined");

      assertTrue(peer2.hasJoined());

      view = peer.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));

      ping = peer2.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));

      peer3.join();
      log.debug("peer3 has joined");

      assertTrue(peer3.hasJoined());

      view = peer.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer2.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer2.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer3.getView();
      assertEquals(3, view.size());
      assertTrue(view.contains(peerIdentity));
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(3, ping.size());
      assertTrue(ping.contains(peerIdentity));
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      peer.leave();
      log.debug("peer has left");

      assertFalse(peer.hasJoined());

      view = peer.getView();
      assertTrue(view.isEmpty());

      ping = peer.ping();
      assertTrue(view.isEmpty());

      view = peer2.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer2.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      view = peer3.getView();
      assertEquals(2, view.size());
      assertTrue(view.contains(peer2Identity));
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(2, ping.size());
      assertTrue(ping.contains(peer2Identity));
      assertTrue(ping.contains(peer3Identity));

      peer2.leave();
      log.debug("peer2 has left");

      assertFalse(peer2.hasJoined());

      view = peer2.getView();
      assertTrue(view.isEmpty());

      ping = peer2.ping();
      assertTrue(view.isEmpty());

      view = peer3.getView();
      assertEquals(1, view.size());
      assertTrue(view.contains(peer3Identity));

      ping = peer3.ping();
      assertEquals(1, ping.size());
      assertTrue(ping.contains(peer3Identity));

      peer3.leave();
      log.debug("peer3 has left");

      assertFalse(peer3.hasJoined());

      view = peer3.getView();
      assertTrue(view.isEmpty());

      ping = peer3.ping();
      assertTrue(view.isEmpty());
   }



   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected abstract Distributed createDistributed(String name,
                                                    MessageStore ms,
                                                    RpcDispatcher d);

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
