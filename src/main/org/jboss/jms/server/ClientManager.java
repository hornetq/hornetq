/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.io.Serializable;

import org.jboss.jms.server.endpoint.ServerConnectionDelegate;

/**
 * A ClientManager manages client connections
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface ClientManager
{

   ServerConnectionDelegate putConnectionDelegate(Serializable connectionID,
                                                  ServerConnectionDelegate d);

   void removeConnectionDelegate(Serializable connectionID);

   ServerConnectionDelegate getConnectionDelegate(Serializable connectionID);
   
}
