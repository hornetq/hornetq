/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.management;

import org.jboss.jms.DestinationImpl;
import org.jboss.jms.server.DeliveryHandler;
import org.jboss.jms.server.SimpleConsumerGroup;
import org.jboss.jms.server.SimpleMessageStore;
import org.jboss.system.ServiceMBeanSupport;

import javax.management.ObjectName;
import javax.naming.InitialContext;

/**
 * @jmx:mbean extends="org.jboss.system.ServiceMBean
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class Destination
        extends ServiceMBeanSupport
        implements DestinationMBean
{
    private String jndiName = null;
    private String deliveryHandler = null;
    private String parentName = null;
    private DestinationImpl destinationName = null;

    public void startService() throws Exception
    {
        Class deliveryHandlerClass =
                Thread.currentThread().getContextClassLoader().loadClass(
                        this.deliveryHandler);
        DeliveryHandler deliveryHandler =
                (DeliveryHandler) deliveryHandlerClass.newInstance();

        org.jboss.jms.server.Destination destination =
                new org.jboss.jms.server.Destination(
                        new SimpleMessageStore(),
                        deliveryHandler,
                        new SimpleConsumerGroup());

        this.destinationName = new DestinationImpl(this.jndiName);

        this.server.invoke(
                new ObjectName("jboss.jms:service=Server"),
                "addDestination",
                new Object[]{this.destinationName, destination},
                new String[]{
                    "org.jboss.jms.DestinationImpl",
                    "org.jboss.jms.server.Destination"});

        new InitialContext().rebind(this.jndiName, destinationName);
    }

    public void stopService() throws Exception
    {
        new InitialContext().unbind(this.jndiName);
        this.server.invoke(
                new ObjectName("jboss.jms:service=Server"),
                "removeDestination",
                new Object[]{this.destinationName},
                new String[]{"org.jboss.jms.DestinationImpl"});
    }

    /**
     * @jmx:managed-attribute
     */
    public void setJndiName(String jndiName) throws IllegalStateException
    {
        this.throwExceptionIfNotStopped();
        this.jndiName = jndiName;
    }

    /**
     * @jmx:managed-attribute
     */
    public String getJndiName()
    {
        return this.jndiName;
    }

    /**
     * @jmx:managed-attribute
     */
    public void setDeliveryHandler(String deliveryHandler)
    {
        this.throwExceptionIfNotStopped();
        this.deliveryHandler = deliveryHandler;
    }

    /**
     * @jmx:managed-attribute
     */
    public String getDeliveryHandler()
    {
        return this.deliveryHandler;
    }

    /**
     * @jmx:managed-attribute
     */
    public void setParentName(String parentName) throws IllegalStateException
    {
        this.throwExceptionIfNotStopped();
        this.parentName = parentName;
    }

    /**
     * @jmx:managed-attribute
     */
    public String getParentName()
    {
        return this.parentName;
    }

    private void throwExceptionIfNotStopped() throws IllegalStateException
    {
        if (this.getState() != ServiceMBeanSupport.STOPPED)
        {
            throw new IllegalStateException("You may not modify the destination while it is running.  Please stop it first, then you may apply changes.");
        }
    }
}