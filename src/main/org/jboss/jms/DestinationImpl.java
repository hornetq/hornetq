/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import java.io.Serializable;

/**
 * As the specification implies, this would be more aptly named "DestinationName"
 * because it (like {@link ObjectName}) simply serves as a value object
 * bound to JNDI, looked up and provided to the client, then passed to the
 * server to indentify the {@link Destination}.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class DestinationImpl implements Serializable, Topic, Queue
{
    private String name = null;

    public DestinationImpl(String name)
    {
        this.name = name;
    }

    public String getTopicName() throws JMSException
    {
        return this.name;
    }

    public String getQueueName() throws JMSException
    {
        return this.name;
    }

    public int hashCode()
    {
        return this.name.hashCode();
    }

    public boolean equals(Object object)
    {
        try
        {
            DestinationImpl destination = (DestinationImpl) object;
            return this.name.equals(destination.name);
        }
        catch (ClassCastException exception)
        {
            return false;
        }
    }

    public String toString()
    {
        return this.getClass().getName() + " [" + this.name + "]";
    }
}
