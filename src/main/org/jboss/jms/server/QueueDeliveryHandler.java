/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

/**
 * Point-to-point messaging model delivery implementation.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class QueueDeliveryHandler extends AbstractDeliveryHandler
{

    public void deliver(
            MessageReference messageReference,
            ConsumerGroup consumers)
    {
        if (!consumers.isEmpty())
        {
            Consumer consumer = consumers.getNext();
            consumer.queueForDelivery(messageReference);
            if (consumer.isActive())
            {
                super.destination.scheduleDelivery(consumer);
            }
        }
        else
        {
            messageReference.save();
        }
    }
}