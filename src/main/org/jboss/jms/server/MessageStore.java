/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.jms.MessageImpl;

import java.util.List;

/**
 * Provides storage of <code>Messages</code> for one or more {@link Destination}.
 * Implementations my implement any manner of storage, caching, etc. or any
 * combination thereof.  However, <code>Messages</code> maked as
 * <code>persistant</code> are expected to be logged to stable storage and
 * survive server failure and/or shutdown and restoration.  Finally, this is
 * soley responsible for <code>Message</code> expiration as the process is
 * inherently dependant upon where <code>Messages</code> are stored.  When
 * {@link #getMessage(Object)} is invoked and the referenced <code>Message</code>
 * has been expired by this, it should throw a {@link MessageExpiredException}.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:tadaley@swbell.net">Troy Daley</a>
 * @version $Revision$ $Date$
 */
public interface MessageStore
{
    public MessageReference store(MessageImpl message);

    public void purge(Object id);

    public MessageImpl getMessage(Object id);

    public List getSavedMessages();

    public void setDelivered(Object id);

    public void setQueued(Object id);
}