/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @version <tt>$Revision$</tt>
 */
public interface PacketDispatcher
{
   void register(PacketHandler handler);

   void unregister(long handlerID);

   void setListener(PacketHandlerRegistrationListener listener);

   void dispatch(Packet packet, PacketReturner sender) throws Exception;

   /**
    * Call filters on a package
    */
   void callFilters(Packet packet) throws Exception;

   long generateID();

   void addInterceptor(Interceptor filter);

   void removeInterceptor(Interceptor filter);
}