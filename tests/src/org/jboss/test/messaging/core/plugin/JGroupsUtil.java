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


package org.jboss.test.messaging.core.plugin;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JGroupsUtil
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   
   public static String getDataStackProperties(int PING_timeout,
                                               int PING_num_initial_members)
   {
      String host = System.getProperty("test.bind.address");
      if (host == null)
      {
         host = "localhost";
      }

      return
         "UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;bind_addr=" + host + "):" +
         "PING(timeout=" + PING_timeout + ";num_initial_members=" + PING_num_initial_members + "):"+
         "FD(timeout=3000):"+
         "VERIFY_SUSPECT(timeout=1500):"+
         "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):"+
         "pbcast.STABLE(desired_avg_gossip=10000):"+
         "FRAG:"+
         "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=true;print_local_addr=true)"; 
   }
   

   
   /*
    * The control stack is used for sending control messages and maintaining state
    * It must be reliable and have the state protocol enabled
    */
   public static String getControlStackProperties(int PING_timeout,
                                                  int PING_num_initial_members)
   {
      String host = System.getProperty("test.bind.address");
      if (host == null)
      {
         host = "localhost";
      }

      return
         "UDP(mcast_addr=228.8.8.8;mcast_port=45568;ip_ttl=32;bind_addr=" + host + "):" +
         "PING(timeout=" + PING_timeout + ";num_initial_members=" + PING_num_initial_members + "):"+
         "FD(timeout=3000):"+
         "VERIFY_SUSPECT(timeout=1500):"+
         "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):"+
         "pbcast.STABLE(desired_avg_gossip=10000):"+
         "FRAG:"+
         "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=true;print_local_addr=true):" +
         "pbcast.STATE_TRANSFER";     
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
