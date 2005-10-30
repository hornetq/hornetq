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
package org.jboss.jms.serverless.jndi;

import org.jboss.logging.Logger;
import javax.naming.Context;
import java.util.Hashtable;
import javax.naming.NamingException;
import java.util.Enumeration;
import javax.naming.InitialContext;
import javax.naming.spi.InitialContextFactory;
import java.util.Iterator;
import java.net.URL;

/**
 * 
 * A JNDI InitialContextFactory implementation. Produces InitalContext instances to be used to
 * lookup JMS administered objects in a namespace associated with a JG group.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class GroupInitialContextFactory implements InitialContextFactory {

    private static final Logger log = Logger.getLogger(GroupInitialContextFactory.class);

    public Context getInitialContext(Hashtable environment) throws NamingException {

        //printEnvironment(environment);
        return new GroupContext(environment);
    }

    static void printEnvironment(Hashtable env) {

        if (env == null) {
            log.info("Null environment");
            return;
        }
        for(Iterator i = env.keySet().iterator(); i.hasNext(); ) {
            Object o = i.next();
            log.info(o+" -> "+env.get(o));
        }
    }
}
