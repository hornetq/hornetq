/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.client.impl;

import java.util.Set;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.remoting.RemotingConnection;

/**
 * 
 * A ClientConnectionInternal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientConnectionInternal extends ClientConnection
{
   RemotingConnection getRemotingConnection();

   void addSession(ClientSessionInternal session);
   
   void removeSession(ClientSessionInternal session);
   
   Set<ClientSession> getSessions();
}
