/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.io.IOException;

import org.jboss.messaging.core.client.RemotingSessionListener;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface NIOConnector
{
   NIOSession connect() throws IOException;

   boolean disconnect();

   void addSessionListener(RemotingSessionListener listener);

   void removeSessionListener(RemotingSessionListener listener);

   String getServerURI();
}