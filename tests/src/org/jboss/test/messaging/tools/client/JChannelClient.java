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
package org.jboss.test.messaging.tools.client;

import org.jgroups.JChannel;
import org.jgroups.ChannelListener;
import org.jgroups.Channel;
import org.jgroups.Address;
import org.jgroups.View;

/**
 * Class that provides a command line interface to a JGroups JChannel. Can be extended for more
 * ellaborated use cases. Run it with Clester.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JChannelClient
{
   // Attributes ----------------------------------------------------

   private String props =
         "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32):"+
         "PING(timeout=3050;num_initial_members=6):"+
         "FD(timeout=3000):"+
         "VERIFY_SUSPECT(timeout=1500):"+
         "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):"+
         "UNICAST(timeout=600,1200,2400,4800):"+
         "pbcast.STABLE(desired_avg_gossip=10000):"+
         "FRAG:"+
         "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=true;print_local_addr=true)";

   protected JChannel jChannel;

   // Constructors --------------------------------------------------

   public JChannelClient() throws Exception
   {
      jChannel = new JChannel(props);
      // this may be replaced by other listener by subclasses
      // TODO commented out to get rid of deprecated warnings at compilation. If needed, replace with something valid
      //jChannel.setChannelListener(new ChannelListenerImpl());

   }

   // Public --------------------------------------------------------

   public String getProperties()
   {
      return jChannel.getProperties();
   }

   public void connect(String groupName) throws Exception
   {
      jChannel.connect(groupName);
   }

   public View getView()
   {
      return jChannel.getView();
   }

   public void exit()
   {
      System.exit(0);
   }

   // Inner classes -------------------------------------------------

   private class ChannelListenerImpl implements ChannelListener
   {
      public void channelConnected(Channel channel)
      {
         System.out.println("Channel connected");
      }

      public void channelDisconnected(Channel channel)
      {
         System.out.println("Channel disconnected");
      }

      public void channelClosed(Channel channel)
      {
         System.out.println("Channel closed");
      }

      public void channelShunned()
      {
         System.out.println("Channel shunned");
      }

      public void channelReconnected(Address addr)
      {
         System.out.println("Channel reconnected");
      }
   }



}
