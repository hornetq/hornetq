/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface NIOSession
{

   long getID();

   void write(Object object);

   Object writeAndBlock(AbstractPacket packet, long timeout,
         TimeUnit timeUnit) throws Throwable;

   boolean isConnected();
}