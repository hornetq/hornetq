/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class TextMessageImpl extends MessageImpl implements TextMessage
{

    TextMessageImpl()
    {
        this.type = MessageImpl.TEXT_MESSAGE_NAME;
    }

    TextMessageImpl(String text)
    {
        this.type = MessageImpl.TEXT_MESSAGE_NAME;
        this.body = text;
    }

    public String getText()
    {
        return (String) this.body;
    }

    public void setText(String text) throws JMSException
    {
        this.throwExceptionIfReadOnly();
        super.body = text;
    }
}