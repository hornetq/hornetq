/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.test.messaging.jms.interception;

import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.util.Logger;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

public class DummyInterceptorB implements Interceptor
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
   
   public void intercept(Packet packet) throws MessagingJMSException
   {
      syncCounter.add(1);
      log.info("DummyFilter packet = " + packet);
   }

}
