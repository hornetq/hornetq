/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.message;

import org.jboss.messaging.jms.message.MessageImpl;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import java.io.Serializable;

/**
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ObjectMessageImpl extends MessageImpl implements ObjectMessage
{

    ObjectMessageImpl()
    {
        super.type = MessageImpl.OBJECT_MESSAGE_NAME;
    }

    public ObjectMessageImpl(Serializable object)
    {
        super.type = MessageImpl.OBJECT_MESSAGE_NAME;
        super.body = object;
    }

    public Serializable getObject() throws JMSException
    {
        return (Serializable) super.body;
    }

    public void setObject(Serializable object) throws JMSException
    {
        super.throwExceptionIfReadOnly();
        super.body = object;
    }

}