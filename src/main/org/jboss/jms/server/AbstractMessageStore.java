/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.MessageImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Abstracts the creation of {@link MessageReference} object so
 * <code>MessageStore</code> implementations only deal with the well known
 * {@link MessageImpl} interface.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:tadaley@swbell.net">Troy Daley</a>
 * @version $Revision$ $Date$
 */
public abstract class AbstractMessageStore implements MessageStore
{
    public synchronized MessageReference store(MessageImpl message)
    {
        Object id = this.storeMessage(message);
        return this.createMessageReference(id);
    }

    public List getSavedMessages()
    {
        List messageReferences = new ArrayList();
        Map savedMessages = this.getAllSavedMessages();
        Iterator iterator = savedMessages.keySet().iterator();
        while (iterator.hasNext())
        {
            messageReferences.add(this.createMessageReference(iterator.next()));
        }
        return messageReferences;
    }

    public abstract Object storeMessage(MessageImpl message);

    public abstract Map getAllSavedMessages();

    private MessageReference createMessageReference(Object key)
    {
        return new MessageReference(this, key);
    }
}