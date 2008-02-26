/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.remoting.impl.PacketDispatcher;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;

/**
 * A PacketHandler handles packets (as defined by {@link AbstractPacket} and its
 * subclasses).
 * 
 * It must have an ID unique among all PacketHandlers (or at least among those
 * registered into the same RemoteDispatcher).
 * 
 * @see PacketDispatcher#register(PacketHandler)
 * @see PacketDispatcher#unregister(String)
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public interface PacketHandler
{
   /*
    * The advantage to use String as ID is that we can leverage Java 5 UUID to
    * generate these IDs. However theses IDs are 128 bite long and it increases
    * the size of a packet (compared to integer or long).
    * 
    * By switching to Long, we could reduce the size of the packet and maybe
    * increase the performance (to check after some performance tests)
    */
   String getID();

   void handle(Packet packet, PacketSender sender);
}
