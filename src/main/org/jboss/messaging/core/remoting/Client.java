/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.jboss.jms.client.api.FailureListener;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.util.MessagingException;

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

   /**
    * @param packet
    *           The packet which is sent
    * @param oneWay
    *           if the packet must be sent one-way (i.e. do not wait for a
    *           response)
    * @return an {@link AbstractPacket} (if <code>oneWay</code> was set to
    *         <code>false</code>) or <code>null</code> (if
    *         <code>oneWay</code> was set to <code>true</code>)
    * 
    * @throws MessagingException
    *            if an exception has occured on the server
    * @throws IOException
    *            if an exception has occured on the network
    */
   AbstractPacket send(AbstractPacket packet, boolean oneWay)
         throws Exception;

   void setBlockingRequestTimeout(int timeout, TimeUnit unit);

   void setFailureListener(final FailureListener listener);
   
   FailureListener getFailureListener();
}