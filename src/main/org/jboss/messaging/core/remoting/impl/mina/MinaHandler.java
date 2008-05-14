/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandlerRegistrationListener;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;
import org.jboss.messaging.util.OrderedExecutorFactory;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class MinaHandler extends IoHandlerAdapter implements
      PacketHandlerRegistrationListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaHandler.class);

   private static boolean trace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final PacketDispatcher dispatcher;

   private CleanUpNotifier failureNotifier;

   private final boolean closeSessionOnExceptionCaught;

   private final OrderedExecutorFactory executorFactory;

   // Note! must use ConcurrentMap here to avoid race condition
   private final ConcurrentMap<Long, Executor> executors = new ConcurrentHashMap<Long, Executor>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   public MinaHandler(final PacketDispatcher dispatcher,
         final ExecutorService executorService,
         final CleanUpNotifier failureNotifier,
         final boolean closeSessionOnExceptionCaught)
   {
      assert dispatcher != null;
      assert executorService != null;

      this.dispatcher = dispatcher;
      this.failureNotifier = failureNotifier;
      this.closeSessionOnExceptionCaught = closeSessionOnExceptionCaught;

      this.executorFactory = new OrderedExecutorFactory(executorService);
      this.dispatcher.setListener(this);
   }

   // Public --------------------------------------------------------

   // PacketHandlerRegistrationListener implementation --------------

   public void handlerRegistered(final long handlerID)
   {
      // do nothing on registration
   }

   public void handlerUnregistered(final long handlerID)
   {
      executors.remove(handlerID);
   }

   // IoHandlerAdapter overrides ------------------------------------

   @Override
   public void exceptionCaught(final IoSession session, final Throwable cause)
         throws Exception
   {
      log.error("caught exception " + cause + " for session " + session, cause);

      if (failureNotifier != null)
      {
         long serverSessionID = session.getId();
         MessagingException me = new MessagingException(
               MessagingException.INTERNAL_ERROR, "unexpected exception");
         me.initCause(cause);
         failureNotifier.fireCleanup(serverSessionID, me);
      }
      if (closeSessionOnExceptionCaught)
      {
         session.close();
      }
   }

   @Override
   public void messageReceived(final IoSession session, final Object message)
   throws Exception
   {
      final Packet packet = (Packet) message;
      long executorID = packet.getExecutorID();

      Executor executor = executors.get(executorID);
      if (executor == null)
      {
         executor = executorFactory.getOrderedExecutor();

         Executor oldExecutor = executors.putIfAbsent(executorID, executor);

         if (oldExecutor != null)
         {
            //Avoid race
            executor = oldExecutor;
         }
      }

      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               messageReceivedInternal(session, packet);
            } catch (Exception e)
            {
               log.error("unexpected error", e);
            }
         }
      });

   }

   private final int high = 2000;

   private final int low = 500;

   private AtomicInteger count = new AtomicInteger(0);

   private volatile boolean blocked = true;

   @Override
   public void messageSent(final IoSession session, final Object message)
         throws Exception
   {
      int newcount = count.decrementAndGet();

      if (blocked)
      {
         if (newcount == low)
         {
            blocked = false;

            // log.info("unblocking");

            synchronized (this)
            {
               this.notify();
            }
         }
      }

   }

   public void acquireSemaphore() throws Exception
   {
      // if (!sem.tryAcquire(5000, TimeUnit.MILLISECONDS))
      // {
      // throw new IllegalStateException("Timed out");
      // }
      int newcount = count.incrementAndGet();

      if (newcount == high)
      {
         blocked = true;

         // log.info("blocking");

         synchronized (this)
         {
            this.wait(5000);
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void messageReceivedInternal(final IoSession session, Packet packet)
         throws Exception
   {
      PacketReturner returner;

      if (packet.getResponseTargetID() != EmptyPacket.NO_ID_SET)
      {
         returner = new PacketReturner()
         {
            public void send(Packet p) throws Exception
            {
               dispatcher.callFilters(p);
               session.write(p);
            }

            public long getSessionID()
            {
               return session.getId();
            }

            public String getRemoteAddress()
            {
               return session.getRemoteAddress().toString();
            }
         };
      }
      else
      {
         returner = null;
      }

      if (trace) log.trace("received packet " + packet);

      dispatcher.dispatch(packet, returner);
   }

   // Inner classes -------------------------------------------------
}
