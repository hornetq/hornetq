/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.management;

import org.jboss.jms.ConnectionFactoryImpl;
import org.jboss.system.ServiceMBeanSupport;

import javax.naming.InitialContext;

/**
 * @jmx:mbean extends="org.jboss.system.ServiceMBean
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ConnectionFactory
        extends ServiceMBeanSupport
        implements ConnectionFactoryMBean
{
    private String jndiName;
    private String connectorName;

    protected final void startService() throws Exception
    {
        //InvokerLocator invokerLocator =
        //        (InvokerLocator) this.getServer().getAttribute(
        //                new ObjectName(this.connectorName),
        //                "Locator");
        new InitialContext().rebind(
                this.jndiName,
                new ConnectionFactoryImpl());
    }

    protected final void stopService() throws Exception
    {
        new InitialContext().unbind(this.jndiName);
    }

    /**
     * @jmx:managed-attribute
     */
    public final void setJndiName(String name)
    {
        this.jndiName = name;
    }

    /**
     * @jmx:managed-attribute
     */
    public final String getJndiName()
    {
        return this.jndiName;
    }

    /**
     * @jmx:managed-attribute
     */
    //public final void setConnectorName(String name)
    //{
    //    this.connectorName = name;
    //}

    /**
     * @jmx:managed-attribute
     */
    //public final String getConnectorName()
    //{
    //    return this.connectorName;
    //}
}