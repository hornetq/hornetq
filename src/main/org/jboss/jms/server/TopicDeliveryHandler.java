/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.util.Iterator;

/**
 * Publish and subscribe messaging model delivery implementation.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class TopicDeliveryHandler extends AbstractDeliveryHandler
{
    public void deliver(
            MessageReference messageReference,
            ConsumerGroup consumers)
    {
        if (!consumers.isEmpty())
        {
            Iterator iterator = consumers.iterator();
            while (iterator.hasNext())
            {
                Consumer consumer = (Consumer) iterator.next();
                consumer.queueForDelivery(messageReference);
                if (consumer.isActive())
                {
                    super.destination.scheduleDelivery(consumer);
                }
            }
        }
        else
        {
            messageReference.delete();
        }
    }
}