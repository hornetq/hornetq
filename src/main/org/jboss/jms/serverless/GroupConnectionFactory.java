/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless;

import org.jboss.logging.Logger;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.JMSException;
import java.net.URL;

/**
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class GroupConnectionFactory implements ConnectionFactory {

    private static final Logger log = Logger.getLogger(GroupConnectionFactory.class);

    private String stackConfigFileName;

    public GroupConnectionFactory(String stackConfigFileName) {
        this.stackConfigFileName = stackConfigFileName;
    }

    /**
     * The Connection is stopped, but it is active (ready to send and receive traffic), 
     * which means the method throws an exception if the group cannot be contacted for some 
     * reason.
     *
     * @see javax.jms.ConnectionFactory#createConnection()
     *
     **/
    public Connection createConnection() throws JMSException {

        URL url = getClass().getClassLoader().getResource(stackConfigFileName);
        if (url == null) {
            String msg = 
                "The channel configuration file (" + stackConfigFileName + ") not found! "+
                "Make sure it is in classpath.";
            throw new JMSException(msg);
        }

        GroupConnection c = new GroupConnection(url);
        c.connect();
        return c;

    }

    public String toString() {
        return
            getClass().getName().toString()+"@"+Integer.toHexString(hashCode())+"["+
            stackConfigFileName+"]";
    }

    /**
     * The Connection is stopped, but it is active (ready to send and receive traffic), 
     * which means the method throws an exception if the group cannot be contacted for some 
     * reason.
     *
     * @see javax.jms.ConnectionFactory#createConnection(String, String)
     **/
    public Connection createConnection(String userName, String password) throws JMSException {
        throw new NotImplementedException();
    }

}
