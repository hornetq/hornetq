/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.remoting.impl.wireformat.Packet;


/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public interface PacketDispatcher
{

   void register(PacketHandler handler);

   void unregister(String handlerID);
   
   void setListener(PacketHandlerRegistrationListener listener);
   
   void dispatch(Packet packet, PacketSender sender) throws Exception;

   /** Call filters on a package */
   void callFilters(Packet packet) throws Exception;

}