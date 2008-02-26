/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.remoting.impl.wireformat.AbstractPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface NIOSession
{

   String getID();

   void write(Object object) throws Exception;

   Object writeAndBlock(AbstractPacket packet, long timeout, TimeUnit timeUnit) throws Exception;

   boolean isConnected();
}