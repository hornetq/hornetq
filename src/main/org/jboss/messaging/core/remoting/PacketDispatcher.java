/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketDispatcher
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PacketDispatcher.class);

   // Attributes ----------------------------------------------------

   private Map<String, PacketHandler> handlers;

   // Static --------------------------------------------------------

   public static final PacketDispatcher client = new PacketDispatcher();
   public static final PacketDispatcher server = new PacketDispatcher();

   // Constructors --------------------------------------------------

   private PacketDispatcher()
   {
      handlers = new ConcurrentHashMap<String, PacketHandler>();
   }

   // Public --------------------------------------------------------

   public void register(PacketHandler handler)
   {
      assertValidID(handler.getID());
      assert handler != null;
      
      handlers.put(handler.getID(), handler);

      if (log.isDebugEnabled())
      {
         log.debug("registered " + handler + " with ID " + handler.getID());
      }
   }

   public void unregister(String handlerID)
   {
      assertValidID(handlerID);

      handlers.remove(handlerID);
      
      if (log.isDebugEnabled())
      {
         log.debug("unregistered handler for " + handlerID);
      }
   }

   public PacketHandler getHandler(String handlerID)
   {
      assertValidID(handlerID);

      return handlers.get(handlerID);
   }
   
   public void dispatch(AbstractPacket packet, PacketSender sender)
   {
      String targetID = packet.getTargetID();
      PacketHandler handler = getHandler(targetID);
      if (handler != null)
      {
         if (log.isTraceEnabled())
            log.trace(handler + " handles " + packet);

         handler.handle(packet, sender);
      } else
      {
         log.warn("Unhandled packet " + packet);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
