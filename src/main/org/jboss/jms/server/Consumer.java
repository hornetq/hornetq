/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

/**
 * Registered with a {@link Destination}, queues <code>Messages</code> on behalf
 * of individual client-side consumers.  A {@link DeliveryHandler} supplies this
 * with one or more <code>Messages</code> by invoking {@link
 * #queueForDelivery(MessageReference)} and then scheduling it with a server-side
 * thread which invokes {@link #deliver} to actually initiate delivery of the
 * queued <code>Messages</code> to the client.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public interface Consumer extends DeliveryEndpoint
{
    public ConsumerMetaData getMetaData();

    public void queueForDelivery(MessageReference messageReference);

    public boolean isActive();

    public void setActive(boolean value);
}