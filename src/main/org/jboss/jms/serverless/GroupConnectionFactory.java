/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
