/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.*;
import static org.jboss.messaging.core.remoting.Packet.NO_ID_SET;
import org.jboss.messaging.core.remoting.impl.wireformat.EmptyPacket;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

   public static final Logger log = Logger
           .getLogger(PacketDispatcherImpl.class);

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
         if (packet.getType() != EmptyPacket.PROD_RECEIVETOKENS)
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
            filter.intercept(packet);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void dump()
   {
      if (log.isDebugEnabled())
      {
         StringBuffer buf = new StringBuffer("Registered PacketHandlers ("
                 + this + "):\n");
         Iterator<Entry<Long, PacketHandler>> iterator = handlers.entrySet()
                 .iterator();
         while (iterator.hasNext())
         {
            Map.Entry<java.lang.Long, org.jboss.messaging.core.remoting.PacketHandler> entry = (Map.Entry<java.lang.Long, org.jboss.messaging.core.remoting.PacketHandler>) iterator
                    .next();
            buf.append(entry.getKey() + " : " + entry.getValue() + "\n");
         }
         log.debug(buf.toString());
      }
   }
   // Inner classes -------------------------------------------------
}
