/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class SortedSetPriorityQueue implements PriorityQueue
{
    private SortedSet contents = null;

    public SortedSetPriorityQueue()
    {
        this.contents = new TreeSet();
    }

    public SortedSetPriorityQueue(Comparator comparator)
    {
        this.contents = new TreeSet(comparator);
    }

    public void enqueue(Object object)
    {
        this.contents.add(object);
    }

    public void enqueue(Collection collection)
    {
        this.contents.addAll(collection);
    }

    public Object dequeue()
    {
        if (!this.contents.isEmpty())
        {
            Iterator iterator = this.contents.iterator();
            Object object = iterator.next();
            iterator.remove();
            return object;
        }
        else
        {
            return null;
        }
    }

    public Collection dequeue(int maximumItems)
    {
        Collection items = new ArrayList(maximumItems);
        Iterator iterator = this.contents.iterator();
        int i = 0;
        while (iterator.hasNext() && i++ < maximumItems)
        {
            items.add(iterator.next());
            iterator.remove();
        }
        return items;
    }

    public Object peek()
    {
        return this.contents.first();
    }

    public Collection peek(int maximumItems)
    {
        Collection items = new ArrayList(maximumItems);
        Iterator iterator = this.contents.iterator();
        int i = 0;
        while (iterator.hasNext() && i++ < maximumItems)
        {
            items.add(iterator.next());
        }
        return items;
    }

    public void purge()
    {
        this.contents.clear();
    }

    public int size()
    {
        return this.contents.size();
    }

    public boolean isEmpty()
    {
        return this.contents.isEmpty();
    }
}
