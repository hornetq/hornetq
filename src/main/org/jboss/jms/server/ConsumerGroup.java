/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.util.Iterator;

/**
 * <code>List</code> like structure for storing <code>Consumers</code> and
 * exposing them in a circular pattern.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public interface ConsumerGroup
{
    public void add(Consumer consumer);

    public void remove(Consumer consumer);

    public int size();

    public boolean isEmpty();

    public Consumer getNext();

    public Iterator iterator();
}