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


import org.jboss.test.messaging.core.base.ChannelTestBase;
import org.jboss.messaging.core.distributed.Peer;
import org.jboss.messaging.core.distributed.DistributedException;
import org.jboss.messaging.core.distributed.PeerIdentity;
import org.jboss.messaging.core.distributed.util.RpcServer;
import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.Channel;
import org.jgroups.JChannel;
import org.jgroups.blocks.RpcDispatcher;

import java.util.List;


/**
 * The test strategy is to group at this level all peer-related tests. It assumes two distinct
 * JGroups JChannel instances and two channel peers (channel and channel2)
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class ChannelPeerTestBase extends ChannelTestBase
{
   // Constants -----------------------------------------------------

   private static final String props =
         "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32):"+
         "PING(timeout=3050;num_initial_members=6):"+
         "FD(timeout=3000):"+
         "VERIFY_SUSPECT(timeout=1500):"+
         "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):"+
         "UNICAST(timeout=600,1200,2400,4800):"+
         "pbcast.STABLE(desired_avg_gossip=10000):"+
         "FRAG:"+
         "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=true;print_local_addr=true)";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected JChannel jchannel, jchannel2;
   protected RpcDispatcher dispatcher, dispatcher2;

   protected MessageStore ms2;
   protected Channel channel2;

   // Constructors --------------------------------------------------

   public ChannelPeerTestBase(String name)
   {
      super(name);
   }


   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      connectJChannel();
   }

   public void tearDown() throws Exception
   {
      if (jchannel.isOpen())
      {
         jchannel.close();
      }

      super.tearDown();
   }


   public void testJGroupsChannelNotConnected() throws Exception
   {
      Peer peer = (Peer)channel;

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
      Peer peer = (Peer)channel;

      assertTrue(jchannel.isConnected());

      peer.join();

      assertTrue(peer.hasJoined());

      PeerIdentity pid = peer.getPeerIdentity();
      assertEquals(channel.getChannelID(), pid.getDistributedID());
      assertNotNull(pid.getPeerID());
      assertNotNull(pid.getAddress());

      List view = peer.getView();

      assertEquals(1, view.size());
      assertEquals(pid, view.get(0));

//      peer.leave();
//
//      assertFalse(peer.hasJoined());
//
//      pid = peer.getPeerIdentity();
//      assertEquals(channel.getChannelID(), pid.getDistributedID());
//      assertNull(pid.getPeerID());
//      assertNull(pid.getAddress());
//
//      view = peer.getView();
//      assertEquals(0, view.size());
   }

   public void testPeerInGroupOfTwo() throws Exception
   {
      connectJChannel2();

      assertTrue(jchannel.isConnected());
      assertTrue(jchannel2.isConnected());

      try
      {


      }
      finally
      {
         jchannel2.close();
      }
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void connectJChannel() throws Exception
   {
      jchannel = new JChannel(props);
      dispatcher = new RpcDispatcher(jchannel, null, null, new RpcServer());
      jchannel.connect("testGroup");
   }

   private void connectJChannel2() throws Exception
   {
      jchannel2 = new JChannel(props);
      dispatcher2 = new RpcDispatcher(jchannel2, null, null, new RpcServer());
      jchannel2.connect("testGroup");
   }


   // Inner classes -------------------------------------------------
}
