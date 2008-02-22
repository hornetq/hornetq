/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.impl;

import org.jboss.jms.client.api.ClientConnection;
import org.jboss.jms.client.remoting.RemotingConnection;

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
   
   RemotingConnection getRemotingConnection();

   void removeChild(String id);
}
