/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.p2p.naming;

import java.util.Hashtable;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import javax.naming.spi.InitialContextFactory;
import javax.naming.Context;
import javax.naming.NamingException;

/**
 * Simple {@link InitialContextFactory} implementation to enable using the JMS/JBoss Pure P2P.
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class InitialContextFactoryImpl implements InitialContextFactory
{
    private static final Map INITIAL_CONTEXT_MAP = new HashMap();

    public synchronized Context getInitialContext(Hashtable environment) throws NamingException
    {
        try
        {
            Integer key = new Integer(environment.hashCode());
            if (!INITIAL_CONTEXT_MAP.containsKey(key))
            {
                Context context = new ContextImpl(environment);
                INITIAL_CONTEXT_MAP.put(key, context);
                return context;
            }
            else
            {
                return (Context) INITIAL_CONTEXT_MAP.get(key);
            }
        }
        catch (Exception exception)
        {
            throw new NamingException(exception.getMessage());
        }
    }

    static synchronized void unregisterInitialContext(Context context)
    {
        if (INITIAL_CONTEXT_MAP.containsValue(context))
        {
            Iterator iterator = INITIAL_CONTEXT_MAP.keySet().iterator();
            while (iterator.hasNext())
            {
                Object key = iterator.next();
                if (INITIAL_CONTEXT_MAP.get(key) == context)
                {
                    INITIAL_CONTEXT_MAP.remove(key);
                }
            }
        }
    }
}
