/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Simple implementation of {@link ConsumerGroup} backed by a {@link LinkedList}.
 * This implementation is unsynchronized.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class SimpleConsumerGroup implements ConsumerGroup
{
    private List consumers = new LinkedList();
    private int nextIndex = 0;

    public void add(Consumer consumer)
    {
        this.consumers.add(consumer);
    }

    public void remove(Consumer consumer)
    {
        int index = this.consumers.indexOf(consumer);
        this.consumers.remove(consumer);
        // Adjust the nextIndex so we don't skip a consumer.
        if (this.nextIndex == (index + 1))
        {
            this.nextIndex--;
        }
    }

    public int size()
    {
        return this.consumers.size();
    }

    public boolean isEmpty()
    {
        return this.consumers.isEmpty();
    }

    public Consumer getNext()
    {
        return (Consumer) this.consumers.get(this.getNextIndex());
    }

    public Iterator iterator()
    {
        return this.consumers.iterator();
    }

    private int getNextIndex()
    {
        int index = (this.nextIndex) % this.consumers.size();
        this.nextIndex = index + 1;
        return index;
    }
}