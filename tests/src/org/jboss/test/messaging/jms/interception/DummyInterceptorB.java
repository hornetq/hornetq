/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.test.messaging.jms.interception;

import org.jboss.messaging.core.remoting.PacketFilter;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.util.Logger;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

public class DummyInterceptorB implements PacketFilter
{

   protected Logger log = Logger.getLogger(DummyInterceptorB.class);

   static SynchronizedInt syncCounter = new SynchronizedInt(0);
   
   public static int getCounter()
   {
      return syncCounter.get();
   }
   
   public static void clearCounter()
   {
      syncCounter.set(0);
   }

   public boolean filterMessage(AbstractPacket packet, PacketHandler handler,
         PacketSender sender)
   {
      log.info("InterceptorB received " + packet);
      syncCounter.add(1);
      return true;
   }

}
