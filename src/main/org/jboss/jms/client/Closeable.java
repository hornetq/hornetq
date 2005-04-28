/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import javax.jms.JMSException;

/**
 * 
 * Implemented by JMS classes that can be closed
 * 
 * Ported from org.jboss.jms.client.LifeCycle in JBoss 4
 * 
 * @author Tim Fox
 *
 */
public interface Closeable {

    /**
     * 
     * Close the instance
     * 
     * @throws JMSException
     */
    void close() throws JMSException;
    
    /**
     * Tell the instance to prepare to close
     * 
     * @throws JMSException
     */
    void closing() throws JMSException;
        
}
