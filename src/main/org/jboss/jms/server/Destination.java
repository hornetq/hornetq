/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.MessageImpl;

/**
 * Constructed with a {@link MessageStore} and a {@link DeliveryHandler}, this
 * plays the central role in the server architecture.  {@link Consumer}
 * instances are stored at this level and provided the
 * <code>DeliveryHandler</code> during <code>Message</code> delivery.  It is
 * important to note that this implements no domain specific functionality.<br/>
 * <br/>
 * This may have a parent <code>Destination</code> which allows the formation of
 * <code>Destination</code> hierarchies.  When a parent is present, the
 * <code>Consumers</code> defined on this remain independant, however, when
 * delivery occures, the <code>Consumers</code> defined on this, plus the
 * <code>Consumers</code> defined on all parents are provided to the
 * <code>DeliveryHandler</code> for delivery.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class Destination
{
    private Server server = null;
    private Destination parent = null;
    private MessageStore messageStore = null;
    private DeliveryHandler deliveryHandler = null;
    private ConsumerGroup consumerGroup = null;

    public Destination(
            Destination parent,
            MessageStore messageStore,
            DeliveryHandler deliveryHandler,
            ConsumerGroup consumerGroup)
    {
        this.parent = parent;
        this.messageStore = messageStore;
        this.deliveryHandler = deliveryHandler;
        this.deliveryHandler.setDestination(this);
        this.consumerGroup = consumerGroup;
    }

    public Destination(
            MessageStore messageStore,
            DeliveryHandler deliveryHandler,
            ConsumerGroup consumerGroup)
    {
        this(null, messageStore, deliveryHandler, consumerGroup);
    }

    void setServer(Server server)
    {
        this.server = server;
    }

    public synchronized void addConsumer(Consumer consumer)
    {
        this.consumerGroup.add(consumer);
        if (this.consumerGroup.size() == 1)
        {
            this.deliverSavedMessages();
        }
    }

    public synchronized void removeConsumer(Consumer consumer)
    {
        this.consumerGroup.remove(consumer);
    }

    public synchronized void deliver(MessageImpl message)
    {
        MessageReference messageReference = this.messageStore.store(message);
        messageReference.addReference(this);
        this.deliveryHandler.deliver(messageReference, this.consumerGroup);
        messageReference.removeReference(this);
    }

    private void deliverSavedMessages()
    {
        this.deliveryHandler.deliver(
                this.messageStore.getSavedMessages(),
                this.consumerGroup);
    }

    void scheduleDelivery(DeliveryEndpoint deliveryEndpoint)
    {
        this.server.scheduleDelivery(deliveryEndpoint);
    }
}