/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.p2p.naming;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.OperationNotSupportedException;

import org.jgroups.blocks.DistributedTree;
import org.jboss.jms.client.p2p.P2PImplementation;
import org.jboss.messaging.jms.destination.JBossDestination;
import org.jboss.messaging.jms.client.facade.JBossConnectionFactory;

/**
 * Simple {@link Context} implementation to enable using Pure P2P JMS/JBoss in a vendor neutral
 * way.  This uses {@link DistributedTree} to create a single namespace shared by all Pure P2P JMS/
 * JBoss clients defined in the same group.  This is a minimal implementation only built to support
 * the very basic needs of JMS clients--namely looking up instances of
 * {@link javax.jms.ConnectionFactory} and {@link javax.jms.Destination} by thier <code>String</code>
 * names. In fact, all the operations that requre use of {@link Name} will throw
 * {@link OperationNotSupportedException} because I didn't want to get into dealing with the syntax
 * properties, etc.  If it is implemented here, it is either required to support the JMS clients, it
 * was easy so I did it, or partial implementation was easy so I did it. An example of partial
 * implementation is the {@link NamingEnumeration} returned by the {@link #list} methods.  The
 * returned <code>NamingEnumeration</code> is be expected to provide more than the <code>String</code>
 * values of the keys which is what it returns now.<br/>
 * <br/>
 * Additionally, this should be refactored to use a single channel, as right now it creates its
 * own.  That means that right now for a JMS client that uses JNDI, two distinct channels will be
 * opened. This is currently required becuase of the use of the JavaGroup blocks which would step
 * one one another ({@link org.jgroups.blocks.PullPushAdapter} taking messages meeded buy the
 * {@link DistributedTree}, etc.).
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ContextImpl implements Context
{
    private String contextName = null;
    private Hashtable environment = null;
    private DistributedTree jndiTree;

    ContextImpl(Hashtable environment) throws Exception
    {
        this.environment = environment;
        String properties = "UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=0):" + "PING(timeout=5000;num_initial_members=6):" + "FD_SOCK:" + "VERIFY_SUSPECT(timeout=1500):" + "pbcast.STABLE(desired_avg_gossip=10000):" + "pbcast.NAKACK(gc_lag=5;retransmit_timeout=3000):" + "UNICAST(timeout=5000):" + "FRAG(down_thread=false;up_thread=false):" + "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" + "shun=false;print_local_addr=true):" + "pbcast.STATE_TRANSFER";
        this.jndiTree = new DistributedTree("org.jboss.jms.p2p.naming", properties);
        this.jndiTree.start();
        this.init();
    }

    private ContextImpl(Hashtable environment, DistributedTree jndiTree, String contextName)
    {
        this.environment = environment;
        this.jndiTree = jndiTree;
        this.contextName = contextName;
    }

    public Object lookup(Name name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public Object lookup(String name) throws NamingException
    {
        String fullyQualifiedName = this.createName(name);
        Object object = this.jndiTree.get(fullyQualifiedName);
        if (object == null)
        {
            throw new NamingException("Name '" + fullyQualifiedName + "' not bound to JNDI Tree.");
        }
        return object;
    }

    public void bind(Name name, Object object) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public void bind(String name, Object object) throws NamingException
    {
        String fullyQualifiedName = this.createName(name);
        if (this.jndiTree.get(fullyQualifiedName) != null)
        {
            throw new NamingException("The name '" + fullyQualifiedName + "' is already bound to the tree.");
        }
        this.jndiTree.add(fullyQualifiedName, (Serializable) object);
    }

    public void rebind(Name name, Object object) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public void rebind(String name, Object object) throws NamingException
    {
        String fullyQualifiedName = this.createName(name);
        if (this.jndiTree.get(fullyQualifiedName) == null)
        {
            this.jndiTree.add(fullyQualifiedName, (Serializable) object);
        }
        else
        {
            this.jndiTree.set(fullyQualifiedName, (Serializable) object);
        }
    }

    public void unbind(Name name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public void unbind(String name) throws NamingException
    {
        String fullyQualifiedName = this.createName(name);
        this.jndiTree.remove(fullyQualifiedName);
    }

    public void rename(Name oldName, Name newName) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public void rename(String oldName, String newName) throws NamingException
    {
        String fullyQualifiedName = this.createName(oldName);
        Object object = this.lookup(fullyQualifiedName);
        this.unbind(fullyQualifiedName);
        fullyQualifiedName = this.createName(newName);
        this.bind(fullyQualifiedName, object);
    }

    public NamingEnumeration list(Name name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public NamingEnumeration list(String name) throws NamingException
    {
        String fullyQualifiedName = this.createName(name);
        Enumeration enumeration = this.jndiTree.getChildrenNames(fullyQualifiedName).elements();
        return new NamingEnumerationImpl(enumeration);
    }

    public NamingEnumeration listBindings(Name name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public NamingEnumeration listBindings(String name) throws NamingException
    {
        return this.list(name);
    }

    public void destroySubcontext(Name name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public void destroySubcontext(String name) throws NamingException
    {
        String fullyQualifiedName = this.createName(name);
        this.jndiTree.remove(fullyQualifiedName);
    }

    public Context createSubcontext(Name name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public Context createSubcontext(String name) throws NamingException
    {
        String fullyQualifiedName = this.createName(name);
        this.jndiTree.add(fullyQualifiedName);
        return new ContextImpl(this.environment, this.jndiTree, fullyQualifiedName);
    }

    public Object lookupLink(Name name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public Object lookupLink(String name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public NameParser getNameParser(Name name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public NameParser getNameParser(String name) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public Name composeName(Name name, Name prefix) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public String composeName(String name, String prefix) throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public Object addToEnvironment(String name, Object value) throws NamingException
    {
        return this.environment.put(name, value);
    }

    public Object removeFromEnvironment(String name) throws NamingException
    {
        return this.environment.remove(name);
    }

    public Hashtable getEnvironment() throws NamingException
    {
        return this.environment;
    }

    public void close() throws NamingException
    {
        if (this.jndiTree != null && this.contextName == null)  // Don't stop unless this is the InitialContext.
        {
            this.jndiTree.stop();
            InitialContextFactoryImpl.unregisterInitialContext(this);
        }
    }

    public String getNameInNamespace() throws NamingException
    {
        throw new OperationNotSupportedException();
    }

    public void finalize() throws Throwable
    {
        this.close();
        super.finalize();
    }

    private String createName(String name) throws NamingException
    {
        if (name == null)
        {
            throw new NamingException("The name supplied is null.");
        }
        if (this.contextName == null)
        {
            return name;
        }
        else
        {
            return this.contextName + "/" + name;
        }
    }

    private void init() throws Exception
    {
        InputStream propertyFileInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("p2p.properties");
        if (propertyFileInputStream == null)
        {
            // load default
            propertyFileInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("org/jboss/jms/p2p/p2p.properties");
        }
        Properties properties = new Properties();
        properties.load(propertyFileInputStream);

        // Since each client may declare a connection factory we'll just do a rebind.
        this.rebind(properties.getProperty("org.jboss.jms.p2p.connectionfactory.name", "ConnectionFactory"), new JBossConnectionFactory(new P2PImplementation()));

        // Now load any declared destinations, right now just do topics
        String declaredTopics = properties.getProperty("org.jboss.jms.p2p.destinations.topics.names");
        if (declaredTopics != null)
        {
            StringTokenizer tokenizer = new StringTokenizer(declaredTopics, ",", false);
            while (tokenizer.hasMoreTokens())
            {
                String currentToken = tokenizer.nextToken();
                // TOD: Need to validate that the name includes no illegal characters
                // TOD: Should I be doing a rebind for destinations?
                this.rebind(currentToken, new JBossDestination(currentToken));
            }
        }
    }

    private class NamingEnumerationImpl implements NamingEnumeration
    {
        private Enumeration enumeration = null;

        NamingEnumerationImpl(Enumeration enumeration)
        {
            this.enumeration = enumeration;
        }

        public Object next() throws NamingException
        {
            return this.nextElement();
        }

        public boolean hasMore() throws NamingException
        {
            return this.hasMoreElements();
        }

        public void close() throws NamingException
        {
            this.enumeration = null;
        }

        public boolean hasMoreElements()
        {
            return this.enumeration.hasMoreElements();
        }

        public Object nextElement()
        {
            return this.enumeration.nextElement();
        }
    }
}