/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server;


/**
 * A factory for browser endpoints
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public interface BrowserEndpointFactory
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   BrowserEndpoint getBrowserEndpoint();
}
