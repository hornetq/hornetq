/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.util;

import java.util.Collection;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public interface PriorityQueue
{
    public void enqueue(Object object);

    public void enqueue(Collection collection);

    public Object dequeue();

    public Collection dequeue(int maximumItems);

    public Object peek();

    public Collection peek(int maximumItems);

    public void purge();

    public int size();

    public boolean isEmpty();
}
