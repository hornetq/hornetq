/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.util.Collection;
import java.util.Iterator;

/**
 * <code>DeliveryHandler</code> which simply handles delivery of
 * <code>MessageReference</code> <code>Collections</code> by iterating
 * through the <code>Collection</code> and invoking {@link
 * #deliver(MessageReference, ConsumerGroup)} on the concrete subclass.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public abstract class AbstractDeliveryHandler implements DeliveryHandler
{
    protected Destination destination = null;

    public void setDestination(Destination destination)
    {
        this.destination = destination;
    }

    public void deliver(Collection messages, ConsumerGroup consumers)
    {
        Iterator iterator = messages.iterator();
        while (iterator.hasNext())
        {
            this.deliver((MessageReference) iterator.next(), consumers);
        }
    }
}