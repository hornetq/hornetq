/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.MessageImpl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Simple development MessageStore implementation.  This does not log
 * persistant messges to stable storage or expire messages.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:tadaley@swbell.net">Troy Daley</a>
 * @version $Revision$ $Date$
 */
public class SimpleMessageStore extends AbstractMessageStore
{
    private Map messageStore = new HashMap();
    private long identity = 0;

    public Object storeMessage(MessageImpl message)
    {
        Long id = new Long(this.identity++);
        this.messageStore.put(id, new MessageEntry(message));
        return id;
    }

    public Map getAllSavedMessages()
    {
        Map messages = new HashMap();
        Iterator iterator = this.messageStore.keySet().iterator();
        while (iterator.hasNext())
        {
            Object key = iterator.next();
            MessageEntry entry = (MessageEntry) this.messageStore.get(key);
            if (!entry.isQueued())
            {
                messages.put(key, entry.getMessage());
            }
        }
        return messages;
    }

    public MessageImpl getMessage(Object id)
    {
        return ((MessageEntry) this.messageStore.get(id)).getMessage();
    }

    public void setDelivered(Object id)
    {
        ((MessageEntry) this.messageStore.get(id)).incrementDeliveryCount();
    }

    public void setQueued(Object id)
    {
        ((MessageEntry) this.messageStore.get(id)).incrementQueuedCount();
    }

    public synchronized void purge(Object id)
    {
        this.messageStore.remove(id);
    }

    private class MessageEntry
    {
        private MessageImpl message = null;
        private int deliveryCount = 0;
        private int queuedCount = 0;

        MessageEntry(MessageImpl message)
        {
            this.message = message;
        }

        MessageImpl getMessage()
        {
            return this.message;
        }

        void incrementDeliveryCount()
        {
            this.deliveryCount++;
        }

        int getDeliveryCount()
        {
            return this.deliveryCount;
        }

        boolean isDelivered()
        {
            return this.deliveryCount > 0;
        }

        void incrementQueuedCount()
        {
            this.queuedCount++;
        }

        boolean isQueued()
        {
            return this.queuedCount > 0;
        }

        public boolean equals(Object object)
        {
            return false;
        }
    }
}