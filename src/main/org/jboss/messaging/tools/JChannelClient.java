/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

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
      jChannel.setChannelListener(new ChannelListenerImpl());

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
