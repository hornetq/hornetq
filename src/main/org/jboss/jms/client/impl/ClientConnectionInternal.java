/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.impl;

import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.remoting.MessagingRemotingConnection;

/**
 * 
 * A ClientConnectionInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientConnectionInternal extends ClientConnection
{
   int getServerID();
   
   MessagingRemotingConnection getRemotingConnection();

   void removeChild(String id);
}
