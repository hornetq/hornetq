/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.serverless.jndi;

import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameNotFoundException;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import org.jboss.jms.serverless.GroupConnectionFactory;
import org.jboss.jms.serverless.GroupQueue;
import org.jboss.jms.serverless.GroupTopic;
import org.jboss.jms.serverless.NotImplementedException;
import org.jboss.logging.Logger;

/**
 * A Context that can be used to locally lookup ConnectionFactories and Destinations. It gets
 * the mapping it needs from the JNDI environment (jndi.properties).
 * 
 * @author Ovidiu Feodorov <ovidiu@jboss.org>
 * @version $Revision$ $Date$
 *
 **/
class GroupContext implements Context {

    private static final Logger log = Logger.getLogger(GroupContext.class);

    public static final String PROPERTY_NAME_PREFIX = "jms.serverless.jndi.";
    public static final String CONNECTION_FACTORY_TOKEN = "connectionFactory.";
    public static final String DESTINATION_TOKEN = "destination.";

    // 
    // cache of the environment; the names found in the environment are cached here, so they 
    // can be used to lazy instantiate the administered objects.
    //
    private Map connFactoryInfo; // (jndi name (String) - stack file name (String))
    private Set topicInfo; // jndi name (String)
    private Set queueInfo; // jndi name (String)
        
    //
    // local cache - the instances that have been looked up once are cached here.
    //

    // (connection factory JNDI name (String) - ConnectionFactory instance)
    private Map connFactories; // (jndi name (String) - ConnectionFactory instance)
    private Map topics; // (jndi name (String) - Topic instance)
    private Map queues; // (jndi name (String) - Queue instance)

    /**
     * @param environment - The possibly null JNDI environment, as received by the
     *        InitialContextFactory.
     **/
    GroupContext(Hashtable environment) {

        connFactoryInfo = new HashMap();
        topicInfo = new HashSet();
        queueInfo = new HashSet();
        cacheAdministeredObjectInfo(environment);
        connFactories = new HashMap();
        topics = new HashMap();
        queues = new HashMap();
    }


    /**
     * Extracts the properties associated with SLJMS administered objects and caches them.
     **/
    private void cacheAdministeredObjectInfo(Hashtable environment) {

        if (environment == null) {
            return;
        }

        for(Enumeration e = environment.keys(); e.hasMoreElements(); ) {
            String prop = (String)e.nextElement();
            if (!prop.startsWith(PROPERTY_NAME_PREFIX)) {
                continue;
            }
            String key = prop.substring(PROPERTY_NAME_PREFIX.length());
            if (key.startsWith(CONNECTION_FACTORY_TOKEN)) {
                connFactoryInfo.put(key.substring(CONNECTION_FACTORY_TOKEN.length()),
                                    environment.get(prop));
            }
            else if (key.startsWith(DESTINATION_TOKEN)) {
                String destJNDIName = key.substring(DESTINATION_TOKEN.length());
                String destType = ((String)environment.get(prop)).toLowerCase();
                if ("topic".equals(destType)) {
                    topicInfo.add(destJNDIName);
                }
                else if ("queue".equals(destType)) {
                    queueInfo.add(destJNDIName);
                }
            }
        }
    }

    public Object lookup(Name name) throws NamingException {
        throw new NotImplementedException();
    }

    public Object lookup(String name) throws NamingException {

        // try to fist find it in cache

        Object o = null;
        if ((o = connFactories.get(name)) != null) {
            return o;
        }
        if ((o = topics.get(name)) != null) {
            return o;
        }
        if ((o = queues.get(name)) != null) {
            return o;
        }
        
        // not in cache, build it

        String stackConfigFileName = (String)connFactoryInfo.get(name);
        
        if (stackConfigFileName != null) {
            ConnectionFactory cf = new GroupConnectionFactory(stackConfigFileName);
            connFactories.put(name, cf);
            return cf;
        }
        if (topicInfo.contains(name)) {
            Destination t = new GroupTopic(name);
            topics.put(name, t);
            return t;
        }
        if (queueInfo.contains(name)) {
            Destination q = new GroupQueue(name);
            queues.put(name, q);
            return q;
        }
        throw new NameNotFoundException(name+" not found");
    }

    public void bind(Name name, Object obj) throws NamingException {
        throw new NotImplementedException();
    }

    public void bind(String name, Object obj) throws NamingException { 
        throw new NotImplementedException();
    }

    public void rebind(Name name, Object obj) throws NamingException {
        throw new NotImplementedException();
    }

    public void rebind(String name, Object obj) throws NamingException {
        throw new NotImplementedException();
    }

    public void unbind(Name name) throws NamingException {
        throw new NotImplementedException();
    }

    public void unbind(String name) throws NamingException {
        throw new NotImplementedException();
    }

    public void rename(Name oldName, Name newName) throws NamingException {
        throw new NotImplementedException();
    }

    public void rename(String oldName, String newName) throws NamingException {
        throw new NotImplementedException();
    }

    public NamingEnumeration list(Name name) throws NamingException {
        throw new NotImplementedException();
    }

    public NamingEnumeration list(String name) throws NamingException {
        throw new NotImplementedException();
    }

    public NamingEnumeration listBindings(Name name) throws NamingException {
        throw new NotImplementedException();
    }

    public NamingEnumeration listBindings(String name) throws NamingException {
        throw new NotImplementedException();
    }

    public void destroySubcontext(Name name) throws NamingException {
        throw new NotImplementedException();
    }

    public void destroySubcontext(String name) throws NamingException {
        throw new NotImplementedException();
    }

    public Context createSubcontext(Name name) throws NamingException {
        throw new NotImplementedException();
    }

    public Context createSubcontext(String name) throws NamingException {
        throw new NotImplementedException();
    }

    public Object lookupLink(Name name) throws NamingException {
        throw new NotImplementedException();
    }

    public Object lookupLink(String name) throws NamingException {
        throw new NotImplementedException();
    }

    public NameParser getNameParser(Name name) throws NamingException {
        throw new NotImplementedException();
    }

    public NameParser getNameParser(String name) throws NamingException {
        throw new NotImplementedException();
    }

    public Name composeName(Name name, Name prefix) throws NamingException {
        throw new NotImplementedException();
    }

    public String composeName(String name, String prefix) throws NamingException {
        throw new NotImplementedException();
    }

    public Object addToEnvironment(String propName, Object propVal) throws NamingException {
        throw new NotImplementedException();
    }

    public Object removeFromEnvironment(String propName) throws NamingException {
        throw new NotImplementedException();
    }

    public Hashtable getEnvironment() throws NamingException {
        throw new NotImplementedException();
    }

    public void close() throws NamingException {
        throw new NotImplementedException();
    }

    public String getNameInNamespace() throws NamingException {
        throw new NotImplementedException();
    }

}
