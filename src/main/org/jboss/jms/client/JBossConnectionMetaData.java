/*
 * Created on Mar 29, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jboss.jms.client;

import java.util.Enumeration;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import org.jboss.messaging.util.NotYetImplementedException;

/**
 * Connection metadata
 * 
 * @author Tim Fox
 */
public class JBossConnectionMetaData implements ConnectionMetaData {

    // Constants -----------------------------------------------------

    // Static --------------------------------------------------------

    // Attributes ----------------------------------------------------
    
    // Constructors --------------------------------------------------

    /**
     * Create a new JBossConnectionMetaData object
     * Note that this has package visibility.
     * Only constructed from JBossConnection
     *
     */
    JBossConnectionMetaData() {}
            
    
    // ConnectionMetaData Implementation
    
    public String getJMSVersion() throws JMSException {
        return "1.1";
    }

 
    public int getJMSMajorVersion() throws JMSException {
        return 1;
    }


    public int getJMSMinorVersion() throws JMSException {
        return 1;
    }


    public String getJMSProviderName() throws JMSException {
        return "jboss.org";
    }

 
    public String getProviderVersion() throws JMSException {        
        return null;
    }


    public int getProviderMajorVersion() throws JMSException {
        return 5;
    }


    public int getProviderMinorVersion() throws JMSException {
        return 0;
    }


    public Enumeration getJMSXPropertyNames() throws JMSException {
        throw new NotYetImplementedException();
    }

}
