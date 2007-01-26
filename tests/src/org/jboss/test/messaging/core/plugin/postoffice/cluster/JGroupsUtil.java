/**
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


package org.jboss.test.messaging.core.plugin.postoffice.cluster;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1252 $</tt>
 *
 * $Id: JGroupsUtil.java 1252 2006-09-01 22:07:43Z timfox $
 */
public class JGroupsUtil
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   
   public static String getDataStackProperties()
   {
//      String host = System.getProperty("test.bind.address");
//      if (host == null)
//      {
//         host = "localhost";
//      }

      /*
      
      <UDP mcast_recv_buf_size="500000" down_thread="false" ip_mcast="true" mcast_send_buf_size="32000"
         mcast_port="45567" ucast_recv_buf_size="500000" use_incoming_packet_handler="false"
         mcast_addr="228.8.8.8" use_outgoing_packet_handler="true" loopback="true" ucast_send_buf_size="32000" ip_ttl="32" bind_addr="127.0.0.1"/>
          <AUTOCONF down_thread="false" up_thread="false"/>
          <PING timeout="2000" down_thread="false" num_initial_members="3" up_thread="false"/>
          <MERGE2 max_interval="10000" down_thread="false" min_interval="5000" up_thread="false"/>
          <FD_SOCK down_thread="false" up_thread="false"/>
          <FD timeout="20000" max_tries="3" down_thread="false" up_thread="false" shun="true"/>            
          <VERIFY_SUSPECT timeout="1500" down_thread="false" up_thread="false"/>
          <pbcast.NAKACK max_xmit_size="8192" down_thread="false" use_mcast_xmit="true" gc_lag="50" up_thread="false"
                       retransmit_timeout="100,200,600,1200,2400,4800"/>
          <UNICAST timeout="1200,2400,3600" down_thread="false" up_thread="false"/>
          <pbcast.STABLE stability_delay="1000" desired_avg_gossip="20000" down_thread="false" max_bytes="0" up_thread="false"/>
          <FRAG frag_size="8192" down_thread="false" up_thread="false"/>
          <VIEW_SYNC avg_send_interval="60000" down_thread="false" up_thread="false" />
          <pbcast.GMS print_local_addr="true" join_timeout="3000" down_thread="false" join_retry_timeout="2000" up_thread="false" shun="true"/>
       </config>
       
       */
      
      
      return "UDP(mcast_recv_buf_size=500000;down_thread=false;ip_mcast=true;mcast_send_buf_size=32000;"+
           "mcast_port=45567;ucast_recv_buf_size=500000;use_incoming_packet_handler=false;"+
         "mcast_addr=228.8.8.8;use_outgoing_packet_handler=true;loopback=true;ucast_send_buf_size=32000;ip_ttl=32;"+
         "bind_addr=127.0.0.1):"+
      "AUTOCONF(down_thread=false;up_thread=false):"+
      "PING(timeout=2000;down_thread=false;num_initial_members=3;up_thread=false):"+
      "MERGE2(max_interval=10000;down_thread=false;min_interval=5000;up_thread=false):"+
      "FD_SOCK(down_thread=false;up_thread=false):"+
      "FD(timeout=20000;max_tries=3;down_thread=false;up_thread=false;shun=true):"+
      "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):"+
      "pbcast.NAKACK(max_xmit_size=8192;down_thread=false;use_mcast_xmit=true;gc_lag=50;up_thread=false;"+
                    "retransmit_timeout=100,200,600,1200,2400,4800):"+
      "UNICAST(timeout=1200,2400,3600;down_thread=false;up_thread=false):"+
      "pbcast.STABLE(stability_delay=1000;desired_avg_gossip=20000;down_thread=false;max_bytes=0;up_thread=false):"+
      "FRAG(frag_size=8192;down_thread=false;up_thread=false):"+
      "VIEW_SYNC(avg_send_interval=60000;down_thread=false;up_thread=false):"+
      "pbcast.GMS(print_local_addr=true;join_timeout=3000;down_thread=false;join_retry_timeout=2000;up_thread=false;shun=true)";
   }
   

   
   /*
    * The control stack is used for sending control messages and maintaining state
    * It must be reliable and have the state protocol enabled
    */
   public static String getControlStackProperties()
   {
//      String host = System.getProperty("test.bind.address");
//      if (host == null)
//      {
//         host = "localhost";
//      }
     
      return "UDP(mcast_recv_buf_size=500000;down_thread=false;ip_mcast=true;mcast_send_buf_size=32000;"+
      "mcast_port=45568;ucast_recv_buf_size=500000;use_incoming_packet_handler=false;"+
      "mcast_addr=228.8.8.8;use_outgoing_packet_handler=true;loopback=true;ucast_send_buf_size=32000;ip_ttl=32;"+
      "bind_addr=127.0.0.1):"+
      "AUTOCONF(down_thread=false;up_thread=false):"+
      "PING(timeout=2000;down_thread=false;num_initial_members=3;up_thread=false):"+
      "MERGE2(max_interval=10000;down_thread=false;min_interval=5000;up_thread=false):"+
      "FD_SOCK(down_thread=false;up_thread=false):"+
      "FD(timeout=20000;max_tries=3;down_thread=false;up_thread=false;shun=true):"+
      "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):"+
      "pbcast.NAKACK(max_xmit_size=8192;down_thread=false;use_mcast_xmit=true;gc_lag=50;up_thread=false;"+
      "retransmit_timeout=100,200,600,1200,2400,4800):"+
      "UNICAST(timeout=1200,2400,3600;down_thread=false;up_thread=false):"+
      "pbcast.STABLE(stability_delay=1000;desired_avg_gossip=20000;down_thread=false;max_bytes=0;up_thread=false):"+
      "FRAG(frag_size=8192;down_thread=false;up_thread=false):"+
      "VIEW_SYNC(avg_send_interval=60000;down_thread=false;up_thread=false):"+
      "pbcast.GMS(print_local_addr=true;join_timeout=3000;down_thread=false;join_retry_timeout=2000;up_thread=false;shun=true):"+
      "pbcast.STATE_TRANSFER(down_thread=false;up_thread=false)";
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
