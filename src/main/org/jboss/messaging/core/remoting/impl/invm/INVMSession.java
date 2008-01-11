/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.invm;

import static java.util.UUID.randomUUID;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.remoting.NIOSession;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class INVMSession implements NIOSession
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String id;
   private ExecutorService executor;
   private long correlationCounter;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public INVMSession()
   {
      this.id = randomUUID().toString();
      this.executor = Executors.newSingleThreadExecutor();
      this.correlationCounter = 0;
   }

   // Public --------------------------------------------------------

   public boolean close()
   {
      if (executor.isShutdown())
         return true;
      executor.shutdown();
      return true;
   }

   // NIOSession implementation -------------------------------------

   public String getID()
   {
      return id;
   }

   public boolean isConnected()
   {
      return true;
   }

   public void write(final Object object)
   {
      assert object instanceof AbstractPacket;

      PacketDispatcher.server.dispatch((AbstractPacket) object,
            new PacketSender()
            {

               public void send(AbstractPacket response)
               {
                  PacketDispatcher.client.dispatch(response, null);
               }
            });
   }

   public Object writeAndBlock(final AbstractPacket request,
         long timeout, TimeUnit timeUnit) throws Throwable
   {
      request.setCorrelationID(correlationCounter++);
      Future<AbstractPacket> future = executor
            .submit(new PacketDispatcherCallable(request));
      return future.get(timeout, timeUnit);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private final class PacketDispatcherCallable implements
         Callable<AbstractPacket>
   {
      private final AbstractPacket packet;

      private PacketDispatcherCallable(AbstractPacket packet)
      {
         this.packet = packet;
      }

      public AbstractPacket call() throws Exception
      {
         final CountDownLatch latch = new CountDownLatch(1);
         final AbstractPacket[] responses = new AbstractPacket[1];

         PacketDispatcher.server.dispatch((AbstractPacket) packet,
               new PacketSender()
               {
                  public void send(AbstractPacket response)
                  {
                     responses[0] = response;
                     latch.countDown();
                  }
               });

         latch.await();

         assert responses[0] != null;

         return responses[0];
      }
   }
}
