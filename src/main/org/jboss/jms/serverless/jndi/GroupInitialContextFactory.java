/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
