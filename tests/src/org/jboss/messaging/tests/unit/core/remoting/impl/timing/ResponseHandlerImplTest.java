/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting.impl.timing;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.ResponseHandler;
import org.jboss.messaging.core.remoting.impl.ResponseHandlerImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Ping;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ResponseHandlerImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   protected static final long TIMEOUT = 500;


   public void testReceiveResponseInTime() throws Exception
   {
      long id = randomLong();
      final ResponseHandler handler = new ResponseHandlerImpl(id);

      final AtomicReference<Packet> receivedPacket = new AtomicReference<Packet>();
      final CountDownLatch latch = new CountDownLatch(1);

      Executors.newSingleThreadExecutor().execute(new Runnable() {
         public void run()
         {
            Packet response = handler.waitForResponse(TIMEOUT);
            receivedPacket.set(response);
            latch.countDown();
         }         
      });

      Packet ping = new Ping(id);
      handler.handle(ping, null);

      boolean gotPacketBeforeTimeout = latch.await(TIMEOUT, MILLISECONDS);
      assertTrue(gotPacketBeforeTimeout);
      assertNotNull(receivedPacket.get());
   }

   public void testReceiveResponseTooLate() throws Exception
   {
      final ResponseHandler handler = new ResponseHandlerImpl(randomLong());
      final AtomicReference<Packet> receivedPacket = new AtomicReference<Packet>();

      Executors.newSingleThreadExecutor().execute(new Runnable() {

         public void run()
         {
            Packet response = handler.waitForResponse(TIMEOUT);
            receivedPacket.set(response);
         }         
      });
      // pause for twice the timeout before handling the packet
      Thread.sleep(TIMEOUT * 2);
      handler.handle(new Ping(handler.getID()), null);

      assertNull(receivedPacket.get());
   }

   public void testSetFailed() throws Exception
   {
      ResponseHandler handler = new ResponseHandlerImpl(randomLong());
      handler.setFailed();
      try {
         handler.waitForResponse(TIMEOUT);
         fail("should throw a IllegalStateException");
      } catch (IllegalStateException e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
