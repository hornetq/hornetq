/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import java.util.Enumeration;
import java.util.Properties;

/**
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class ConnectionMetaDataImpl implements ConnectionMetaData
{
    private Properties properties = new Properties();

    public ConnectionMetaDataImpl()
    {
        try
        {
            this.properties.load(
                    Thread
                    .currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(
                            "org/jboss/jms/version.properties"));
        }
        catch (Exception exception)
        {
        }
    }

    public int getJMSMajorVersion() throws JMSException
    {
        return Integer.parseInt(
                this.properties.getProperty("org.jboss.jms.version.major"));
    }

    public int getJMSMinorVersion() throws JMSException
    {
        return Integer.parseInt(
                this.properties.getProperty("org.jboss.jms.version.minor"));
    }

    public String getJMSProviderName() throws JMSException
    {
        return this.properties.getProperty("org.jboss.jms.provider.name");
    }

    public String getJMSVersion() throws JMSException
    {
        return this.properties.getProperty("org.jboss.jms.version.major")
                + "."
                + this.properties.getProperty("org.jboss.jms.version.minor");
    }

    public Enumeration getJMSXPropertyNames() throws JMSException
    {
        return null; //TODO: Implement
    }

    public int getProviderMajorVersion() throws JMSException
    {
        return Integer.parseInt(
                this.properties.getProperty(
                        "org.jboss.jms.provider.version.major"));
    }

    public int getProviderMinorVersion() throws JMSException
    {
        return Integer.parseInt(
                this.properties.getProperty(
                        "org.jboss.jms.provider.version.minor"));
    }

    public String getProviderVersion() throws JMSException
    {
        return this.properties.getProperty(
                "org.jboss.jms.provider.version.major")
                + "."
                + this.properties.getProperty("org.jboss.jms.provider.version.minor")
                + " "
                + this.properties.getProperty("org.jboss.jms.provider.version.tag");
    }

    public String toString()
    {
        try
        {
            return this.getJMSProviderName()
                    + " "
                    + this.getProviderVersion()
                    + "\nJava Message Service "
                    + this.getJMSVersion();
        }
        catch (JMSException exception)
        {
            return null;
        }
    }

}