/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.client.RemotingSessionListener;

import java.io.IOException;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface RemotingConnector
{
   RemotingSession connect() throws IOException;

   boolean disconnect();

   void addSessionListener(RemotingSessionListener listener);

   void removeSessionListener(RemotingSessionListener listener);

   String getServerURI();

   PacketDispatcher getDispatcher();
}