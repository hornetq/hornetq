/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;


/**
 * A PacketHandler handles packets (as defined by {@link Packet} and its
 * subclasses).
 * 
 * It must have an ID unique among all PacketHandlers (or at least among those
 * registered into the same RemoteDispatcher).
 * 
 * @see PacketDispatcher#register(PacketHandler)
 * @see PacketDispatcher#unregister(long)
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public interface PacketHandler
{
   long getID();

   void handle(Packet packet, PacketSender sender);
}
