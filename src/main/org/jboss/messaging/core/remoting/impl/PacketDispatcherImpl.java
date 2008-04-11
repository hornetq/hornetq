/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl;

import static org.jboss.messaging.core.remoting.impl.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NO_ID_SET;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketHandlerRegistrationListener;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketDispatcherImpl implements PacketDispatcher, Serializable
{

   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -4626926952268528384L;

   public static final Logger log = Logger.getLogger(PacketDispatcherImpl.class);

   // Attributes ----------------------------------------------------

   private Map<String, PacketHandler> handlers;
   public List<Interceptor> filters;
   private transient PacketHandlerRegistrationListener listener;

   // Static --------------------------------------------------------

   // public static final PacketDispatcher client = new PacketDispatcher();

   // Constructors --------------------------------------------------

   public PacketDispatcherImpl()
   {
      handlers = new ConcurrentHashMap<String, PacketHandler>();
   }

   public PacketDispatcherImpl(List<Interceptor> filters)
   {
      this();
      this.filters = filters;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.impl.IPacketDispatcher#register(org.jboss.messaging.core.remoting.PacketHandler)
    */
   public void register(PacketHandler handler)
   {
      assertValidID(handler.getID());
      assert handler != null;
      
      handlers.put(handler.getID(), handler);

      if (log.isDebugEnabled())
      {
         log.debug("registered " + handler + " with ID " + handler.getID());
      }
      
      if (listener != null)
         listener.handlerRegistered(handler.getID());
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.remoting.impl.IPacketDispatcher#unregister(java.lang.String)
    */
   public void unregister(String handlerID)
   {
      assertValidID(handlerID);

      PacketHandler handler = handlers.remove(handlerID);
      
      if (log.isDebugEnabled())
      {
         log.debug("unregistered " + handler);
      }
      
      if (listener != null)
         listener.handlerUnregistered(handlerID);
   }
   
   public void setListener(PacketHandlerRegistrationListener listener)
   {
      this.listener = listener;
   }

   public PacketHandler getHandler(String handlerID)
   {
      assertValidID(handlerID);

      return handlers.get(handlerID);
   }
   
   public void dispatch(Packet packet, PacketSender sender) throws Exception
   {
      String targetID = packet.getTargetID();
      if (NO_ID_SET.equals(targetID))
      {
         log.error("Packet is not handled, it has no targetID: " + packet + ": " + System.identityHashCode(packet));
         return;
      }
      PacketHandler handler = getHandler(targetID);
      if (handler != null)
      {
         if (log.isTraceEnabled())
            log.trace(handler + " handles " + packet);

         callFilters(packet);
         handler.handle(packet, sender);

      }
      else
      {
         log.error("Unhandled packet " + packet);
      }
   }

   /** Call filters on a package */
   public void callFilters(Packet packet) throws Exception
   {
     if (filters != null)
     {
        for (Interceptor filter: filters)
        {
           filter.intercept(packet);          
        }
     }
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
