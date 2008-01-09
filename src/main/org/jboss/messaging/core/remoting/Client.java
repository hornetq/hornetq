/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.jboss.jms.client.remoting.ConsolidatedRemotingConnectionListener;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface Client
{

   void connect() throws Exception;

   boolean disconnect() throws Exception;

   boolean isConnected();

   String getURI();

   String getSessionID();

   void sendOneWay(AbstractPacket packet) throws JMSException;

   AbstractPacket sendBlocking(AbstractPacket packet) throws IOException,
         JMSException;

   void setBlockingRequestTimeout(int timeout, TimeUnit unit);

   void addConnectionListener(
         final ConsolidatedRemotingConnectionListener listener);

   void removeConnectionListener(ConsolidatedRemotingConnectionListener listener);
}