/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.remoting.impl.integration;

import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.wireformat.DeliverMessage;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.util.Logger;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedInt;

public class DummyInterceptor implements Interceptor
{
   protected Logger log = Logger.getLogger(DummyInterceptor.class);

   boolean sendException = false;
   boolean changeMessage = false;
   SynchronizedInt syncCounter = new SynchronizedInt(0);
   
   public int getCounter()
   {
      return syncCounter.get();
   }
   
   public void clearCounter()
   {
      syncCounter.set(0);
   }
   
   public void intercept(Packet packet) throws MessagingJMSException
   {
      log.info("DummyFilter packet = " + packet.getClass().getName());
      syncCounter.add(1);
      if (sendException)
      {
         throw new MessagingJMSException("Test");
      }
      if (changeMessage)
      {
         if (packet instanceof DeliverMessage)
         {
            DeliverMessage deliver = (DeliverMessage)packet;
            log.info("msg = " + deliver.getMessage().getClass().getName());
            deliver.getMessage().getHeaders().put("DummyInterceptor", "was here");
         }
      }
   }

}
