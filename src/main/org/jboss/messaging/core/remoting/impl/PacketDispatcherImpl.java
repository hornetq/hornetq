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

package org.jboss.messaging.core.remoting.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.*;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NO_ID_SET;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>.
 * @version <tt>$Revision$</tt>
 */
public class PacketDispatcherImpl implements PacketDispatcher
{

   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -4626926952268528384L;

   public static final Logger log = Logger.getLogger(PacketDispatcherImpl.class);

   private static boolean trace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final Map<Long, PacketHandler> handlers;

   private transient PacketHandlerRegistrationListener listener;

   private final AtomicLong idSequence = new AtomicLong(0);

   private List<Interceptor> filters = new CopyOnWriteArrayList<Interceptor>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PacketDispatcherImpl(final List<Interceptor> filters)
   {
      handlers = new ConcurrentHashMap<Long, PacketHandler>();
      if (filters != null)
      {
         this.filters.addAll(filters);
      }
   }

   // Public --------------------------------------------------------

   public long generateID()
   {
      long id = idSequence.getAndIncrement();

      if (id == 0)
      {
         // ID 0 is reserved for the connection factory handler
         id = generateID();
      }

      return id;
   }

   public void register(final PacketHandler handler)
   {
      handlers.put(handler.getID(), handler);

      if (trace)
      {
         log.trace("registered " + handler + " with ID " + handler.getID()
                 + " (" + this + ")");
      }

      if (listener != null)
      {
         listener.handlerRegistered(handler.getID());
      }
   }

   public void unregister(final long handlerID)
   {
      PacketHandler handler = handlers.remove(handlerID);

      if (handler == null)
      {
         throw new IllegalArgumentException("Failed to unregister handler " + handlerID);
      }
      if (trace)
      {
         log.trace("unregistered " + handler);
      }

      if (listener != null)
      {
         listener.handlerUnregistered(handlerID);
      }
   }

   public void setListener(final PacketHandlerRegistrationListener listener)
   {
      this.listener = listener;
   }

   public PacketHandler getHandler(final long handlerID)
   {
      return handlers.get(handlerID);
   }

   public void addInterceptor(Interceptor filter)
   {
      filters.add(filter);
   }

   public void removeInterceptor(Interceptor filter)
   {
      filters.remove(filter);
   }

   public void dispatch(final Packet packet, final PacketReturner sender)
           throws Exception
   {
      long targetID = packet.getTargetID();
      if (NO_ID_SET == targetID)
      {
         log.error("Packet is not handled, it has no targetID: " + packet
                 + ": " + System.identityHashCode(packet));
         return;
      }
      PacketHandler handler = getHandler(targetID);
      if (handler != null)
      {
         if (trace) log.trace(handler + " handles " + packet);

         callFilters(packet);
         handler.handle(packet, sender);
      }
      else
      {
         //Producer tokens can arrive after producer is closed - this is ok
         if (packet.getType() != PacketImpl.PROD_RECEIVETOKENS)
         {
            log.error("Unhandled packet " + packet);
         }
      }
   }

   /**
    * Call filters on a package
    */
   public void callFilters(Packet packet) throws Exception
   {
      if (filters != null)
      {
         for (Interceptor filter : filters)
         {
            try
            {
               filter.intercept(packet);
            }
            catch (Throwable e)
            {
               log.warn("unable to call interceptor: " + filter, e);
            }
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
