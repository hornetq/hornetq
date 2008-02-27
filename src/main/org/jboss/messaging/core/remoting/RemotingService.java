/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

import org.jboss.messaging.core.client.FailureListener;
import org.jboss.messaging.core.remoting.impl.RemotingConfiguration;
import org.jboss.messaging.core.server.MessagingComponent;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public interface RemotingService extends MessagingComponent
{
   PacketDispatcher getDispatcher();

   RemotingConfiguration getRemotingConfiguration();
   
   void addInterceptor(Interceptor interceptor);

   void removeInterceptor(Interceptor interceptor);

   void addFailureListener(FailureListener listener);

   void removeFailureListener(FailureListener listener);  
}
