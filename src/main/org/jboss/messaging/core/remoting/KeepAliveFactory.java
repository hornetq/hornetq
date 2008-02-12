/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.remoting.wireformat.Ping;
import org.jboss.messaging.core.remoting.wireformat.Pong;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public interface KeepAliveFactory
{

   Ping ping(String sessionID);
   
   Pong pong(String sessionID, Ping ping);

   boolean isPing(String sessionID, Object message);
}
