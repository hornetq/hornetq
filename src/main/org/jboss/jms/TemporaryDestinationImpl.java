/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.TemporaryTopic;
import javax.jms.TemporaryQueue;
import javax.jms.JMSException;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class TemporaryDestinationImpl extends DestinationImpl implements TemporaryTopic, TemporaryQueue
{
    private ConnectionImpl connection = null;
    private static int destinationCount = 1;

    public TemporaryDestinationImpl(ConnectionImpl connection)
    {
        try {
            this.connection = connection;
            super.name = this.connection.getClientID() + destinationCount++;
        }
        catch (JMSException exception){}
    }

    public void delete() throws JMSException
    {
    }

}