/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.util.PriorityQueue;
import org.jboss.jms.util.SortedSetPriorityQueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Places supplied {@link MessageReference} objects in an internal in-memory
 * queue for delivery.  If <code>active</code>, the controlling {@link
 * DeliveryHandler} will schedule this with the server for physical delivery.
 * Once delivery has been accomplished, the delivered <code>MessageReferences
 * </code> are placed in the internal <code>unacknowledgedMessage</code> list
 * until acknowledged.  Only after acknowledgement is the <code>MessageReference
 * </code> removed from the list and this reference to it removed.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class NonDurableConsumer implements Consumer
{
    private boolean active = true;
    private ConsumerMetaData metaData = null;
    protected PriorityQueue queuedMessages = null;
    protected List unacknowledgedMessages = new ArrayList();

    public NonDurableConsumer()
    {
        this(new ConsumerMetaData());
    }

    public NonDurableConsumer(ConsumerMetaData metaData)
    {
        this.metaData = metaData;
        this.queuedMessages =
                new SortedSetPriorityQueue(this.metaData.getMessageComparator());
        if (ConsumerMetaData.isEnabled())
        {
            this.metaData.setActivationTimeMills(System.currentTimeMillis());
        }
    }

    public ConsumerMetaData getMetaData()
    {
        return this.metaData;
    }

    public synchronized void queueForDelivery(MessageReference messageReference)
    {
        messageReference.addReference(this);
        this.queuedMessages.enqueue(messageReference);
        messageReference.setQueued();
    }

    public boolean isActive()
    {
        return this.active;
    }

    public void setActive(boolean value)
    {
        this.active = value;
        if (value)
        {
            this.metaData.setActivationTimeMills(System.currentTimeMillis());
        }
    }

    public synchronized int deliver()
    {
        long deliveryStarted = System.currentTimeMillis();
        Collection dequeuedMessageReferences =
                this.queuedMessages.dequeue(this.queuedMessages.size());
        Iterator iterator = dequeuedMessageReferences.iterator();
        Collection messages = new ArrayList();
        while (iterator.hasNext())
        {
            MessageReference messageReference =
                    (MessageReference) iterator.next();
            messages.add(messageReference.getMessage());
            messageReference.setDelivered();
            iterator.remove();

            /*
             * TOD: Add the messageReference to the unacknowledgedMessages, and
             * handle acknowledgement.  For now, just remove our reference to
             * the message so it can be removed from its store.
             */
            messageReference.removeReference(this);
        }

        /*
         * TOD: Actually deliver the collection of messages
         * We will have a reference to our session's CallBackHandler which will
         * handle remoting.  For now, just print a statement to standard out.
         * Additionally, we need to provide a mechanism to control the size of
         * the collection we are passing of the wire.
         */
        int deliverySize = messages.size();
        messages.clear();
        if (ConsumerMetaData.isEnabled())
        {
            this.metaData.setLastMessageDeliveryTimeMillis(
                    System.currentTimeMillis());
            this.metaData.addMessageDeliveryCount(deliverySize);
            this.metaData.incrementDeliveryOperationCount();
            this.metaData.reaverageDeliveryOperationTimeMillis(
                    System.currentTimeMillis() - deliveryStarted);
            this.metaData.reaverageDeliverySize(deliverySize);
        }
        System.out.println(
                "Delivered " + deliverySize + " messages to client.");
        return deliverySize;
    }
}