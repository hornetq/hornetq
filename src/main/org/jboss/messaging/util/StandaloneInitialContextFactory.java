/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.util;

import org.jboss.logging.Logger;

import javax.naming.Context;
import java.util.Hashtable;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.util.Iterator;

/**
 * A JNDI InitialContextFactory implementation. Produces InitalContext instances to be used for
 * standalone testing of the JMS clients.
 *
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
public class StandaloneInitialContextFactory implements InitialContextFactory {

    private static final Logger log = Logger.getLogger(StandaloneInitialContextFactory.class);

    public Context getInitialContext(Hashtable environment) throws NamingException {

        //printEnvironment(environment);
        return new StandaloneJNDIContext(environment);
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
