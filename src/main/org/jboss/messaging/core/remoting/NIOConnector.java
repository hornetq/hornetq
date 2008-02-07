/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.io.IOException;

import org.jboss.jms.client.api.FailureListener;

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

   void addFailureListener(FailureListener listener);

   void removeFailureListener(FailureListener listener);

   String getServerURI();
}