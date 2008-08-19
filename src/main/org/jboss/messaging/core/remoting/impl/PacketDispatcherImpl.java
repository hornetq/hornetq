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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>.
 * @version <tt>$Revision$</tt>
 */
public class PacketDispatcherImpl implements PacketDispatcher
{

   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(PacketDispatcherImpl.class);

   private static boolean trace = log.isTraceEnabled();
   
   // Reserved for main server handler
   public static final long MAIN_SERVER_HANDLER_ID = 0;    
   
   // Attributes ----------------------------------------------------

   private final Map<Long, PacketHandler> handlers;

   private final AtomicLong idSequence = new AtomicLong(0);

   private final List<Interceptor> interceptors = new CopyOnWriteArrayList<Interceptor>();
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PacketDispatcherImpl(final List<Interceptor> filters)
   {
      handlers = new ConcurrentHashMap<Long, PacketHandler>();
      if (filters != null)
      {
         interceptors.addAll(filters);
      }
   }

   // Public --------------------------------------------------------

   public long generateID()
   {
      long id = idSequence.getAndIncrement();

      if (id == MAIN_SERVER_HANDLER_ID)
      {
         // Reserved ID
         id = idSequence.getAndIncrement();
      }
      
      return id;
   }

   public void register(final PacketHandler handler)
   {
      handlers.put(handler.getID(), handler);
   }

   public void unregister(final long handlerID)
   {
      PacketHandler handler = handlers.remove(handlerID);

      if (handler == null)
      {
         throw new IllegalArgumentException("Failed to unregister handler " + handlerID);
      }
   }

   public PacketHandler getHandler(final long handlerID)
   {
      return handlers.get(handlerID);
   }

   public void addInterceptor(Interceptor filter)
   {
      interceptors.add(filter);
   }

   public void removeInterceptor(Interceptor filter)
   {
      interceptors.remove(filter);
   }

   public List<Interceptor> getInterceptors()
   {
      return new ArrayList<Interceptor>(interceptors);
   }

   public void dispatch(final Object connectionID, final Packet packet) throws Exception
   {
      long targetID = packet.getTargetID();

      PacketHandler handler = getHandler(targetID);

      if (handler != null)
      {
         if (callInterceptors(packet))
         {
            handler.handle(connectionID, packet);
         }
      }
      else
      {
         //Producer tokens and command confirmations can arrive after producer is closed - this is ok
         int type = packet.getType();
         if (type != PacketImpl.PACKETS_CONFIRMED)     
         {
            log.error("Unhandled packet " + packet);
         }
      }
   }

   public boolean callInterceptors(final Packet packet) throws Exception
   {
      if (interceptors != null)
      {
         for (Interceptor interceptor : interceptors)
         {
            try
            {
               if (!interceptor.intercept(packet))
               {
                  return false;
               }
            }
            catch (Throwable e)
            {
               log.warn("Failed in calling interceptor: " + interceptor, e);
            }
         }
      }
      return true;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
