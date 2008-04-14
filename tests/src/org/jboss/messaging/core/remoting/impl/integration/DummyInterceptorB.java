/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.remoting.impl.integration;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;

public class DummyInterceptorB implements Interceptor
{

   protected Logger log = Logger.getLogger(DummyInterceptorB.class);

   static AtomicInteger syncCounter = new AtomicInteger(0);
   
   public static int getCounter()
   {
      return syncCounter.get();
   }
   
   public static void clearCounter()
   {
      syncCounter.set(0);
   }
   
   public void intercept(Packet packet) throws MessagingException
   {
      syncCounter.addAndGet(1);
      log.info("DummyFilter packet = " + packet);
   }

}
