/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import java.util.Enumeration;

import javax.jms.QueueBrowser;
import javax.jms.Queue;
import javax.jms.JMSException;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class QueueBrowserImpl implements QueueBrowser
{
    private SessionImpl session = null;
    private Queue queue = null;

    public QueueBrowserImpl(SessionImpl session, Queue queue, String selector)
    {
        this.session = session;
        this.queue = queue;
    }

    public Queue getQueue() throws JMSException
    {
        return this.queue;
    }

    public String getMessageSelector() throws JMSException
    {
        return null;
    }

    public Enumeration getEnumeration() throws JMSException
    {
        return null;
    }

    public void close() throws JMSException
    {
    }
}
