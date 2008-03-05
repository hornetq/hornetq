/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface RemotingConfiguration
{
   TransportType getTransport();

   String getHost();

   int getPort();

   int getKeepAliveInterval();

   int getKeepAliveTimeout();

   int getTimeout();

   String getKeyStorePath();

   String getKeyStorePassword();

   String getTrustStorePath();

   String getTrustStorePassword();

   boolean isInvmDisabled();

   boolean isSSLEnabled();

   String getURI();
}