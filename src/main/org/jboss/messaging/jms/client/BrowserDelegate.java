/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client;

import javax.jms.JMSException;
import java.util.List;

/**
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface BrowserDelegate extends Lifecycle
{
   /**
    * Browse the messages.
    *
    * @return a list of messages.
    * @throws JMSException for any error.
    */
   List browse() throws JMSException;
}
