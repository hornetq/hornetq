/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.MessageImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a reference to a <code>Message</code> managed by a {@link
 * MessageStore}.  The reference my refer to a <code>Message</code>
 * cached in memory, persisted to disk, or stored in a database.  In short,
 * this has only a direct reference to a {@link MessageStore} which supplies
 * this with a unique key <code>Object</code> used to identify a specific
 * <code>Message</code> within it at creation time.<br/>
 * <br/>
 * Additionally, this keeps track of objects that reference it.  When all
 * objects release thier reference to this, it triggers the deletion of the
 * referenced <code>Message</code> from the <code>MessageStore</code> by
 * invoking {@link MessageStore#purge(Object)}.<br/>
 * <br/>
 * The goal of this architecture is to store a <code>Message</code> as few times
 * as possible (once except with <code>DurableConsumers</code>) by storing the
 * physical <code>Message</code> in a <code>MessageStore</code> and distributing
 * only a <i>reference</i> to the <code>Message</code> to each {@link Consumer}.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:tadaley@swbell.net">Troy Daley</a>
 * @version $Revision$ $Date$
 */
public class MessageReference implements Cloneable
{
    private MessageStore messageStore = null;
    private Object id = null;
    private List references = new ArrayList();
    private boolean save = false;

    public MessageReference(MessageStore messageStore, Object id)
    {
        this.messageStore = messageStore;
        this.id = id;
    }

    public synchronized void addReference(Object reference)
    {
        this.references.add(reference);
    }

    public synchronized void removeReference(Object reference)
    {
        this.references.remove(reference);
        if (this.references.isEmpty())
        {
            this.delete();
        }
    }

    public synchronized void save()
    {
        this.save = true;
    }

    public synchronized void delete()
    {
        if (this.id != null && !this.save)
        {
            this.messageStore.purge(this.id);
        }
        this.save = false;
    }

    public MessageImpl getMessage()
    {
        return this.messageStore.getMessage(this.id);
    }

    public void setDelivered()
    {
        this.messageStore.setDelivered(this.id);
    }

    public void setQueued()
    {
        this.messageStore.setQueued(this.id);
    }

    public boolean equals(Object object)
    {
        try
        {
            MessageReference messageReference = (MessageReference) object;

            /*
             * To be equal not only do the ids have to be equal, the two objets
             * must have the same MessageStore.
             */
            return this.id.equals(messageReference.id)
                    && this.messageStore == messageReference.messageStore;
        }
        catch (ClassCastException exception)
        {
            return false;
        }
    }

    public String toString()
    {
        return this.getClass().getName() + "[id=" + this.id + "]";
    }
}