/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

/**
 * Implementations my be scheduled with the server's worker threads which will
 * invoke {@link #deliver}.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public interface DeliveryEndpoint
{
    public int deliver();
}