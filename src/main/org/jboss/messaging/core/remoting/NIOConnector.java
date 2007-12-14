/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.io.IOException;

import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface NIOConnector
{

   public abstract NIOSession connect() throws IOException;

   public abstract boolean disconnect();

   public abstract void addConnectionListener(
         final ConsolidatedRemotingConnectionListener listener);

   public abstract void removeConnectionListener(
         ConsolidatedRemotingConnectionListener listener);

   public abstract String getServerURI();

}