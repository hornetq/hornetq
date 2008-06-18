/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.remoting.impl.mina;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NO_ID_SET;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.CleanUpNotifier;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandlerRegistrationListener;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.OrderedExecutorFactory;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class MinaHandler extends IoHandlerAdapter implements
        PacketHandlerRegistrationListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaHandler.class);

   private static boolean trace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final PacketDispatcher dispatcher;

   private final CleanUpNotifier failureNotifier;

   private final boolean closeSessionOnExceptionCaught;

   private final ExecutorFactory executorFactory;

   // Note! must use ConcurrentMap here to avoid race condition
   private final ConcurrentMap<Long, Executor> executors = new ConcurrentHashMap<Long, Executor>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaHandler(final PacketDispatcher dispatcher,
                      final ExecutorService executorService,
                      final CleanUpNotifier failureNotifier,
                      final boolean closeSessionOnExceptionCaught,
                      final boolean useExecutor)
   {
      assert dispatcher != null;
      assert executorService != null;

      this.dispatcher = dispatcher;
      this.failureNotifier = failureNotifier;
      this.closeSessionOnExceptionCaught = closeSessionOnExceptionCaught;
      if (useExecutor)
      {
         executorFactory = new OrderedExecutorFactory(executorService);
      }
      else
      {
         executorFactory = null;
      }
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
      
      if (executorFactory != null)
      {
         long executorID = packet.getExecutorID();

         Executor executor = executors.get(executorID);
         if (executor == null)
         {
            executor = executorFactory.getExecutor();

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
               }
               catch (Exception e)
               {
                  log.error("unexpected error", e);
               }
            }
         });
      }
      else
      {
         messageReceivedInternal(session, packet);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void messageReceivedInternal(final IoSession session, Packet packet)
           throws Exception
   {
      PacketReturner returner;

      if (packet.getResponseTargetID() != NO_ID_SET)
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

      if (trace)
      {
         log.trace("received packet " + packet);
      }

      dispatcher.dispatch(packet, returner);
   }

   // Inner classes -------------------------------------------------
}
