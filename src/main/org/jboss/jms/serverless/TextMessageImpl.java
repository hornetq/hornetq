/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import java.io.Serializable;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class TextMessageImpl extends MessageImpl implements TextMessage, Serializable {

    private static final Logger log = Logger.getLogger(TextMessageImpl.class);

    private String text;
    
    public String getText() throws JMSException {
        return text;
    }

    public void setText(String string) throws JMSException {
        text = string;
    }
}
