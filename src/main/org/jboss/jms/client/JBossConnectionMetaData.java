/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.util.Enumeration;

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;

/**
 * Connection metadata
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class JBossConnectionMetaData 
   implements ConnectionMetaData
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The connection for this meta data */
   private ConnectionDelegate connection;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Create a new metadata object
    * 
    * @param connection the connection for this meta data
    */
   public JBossConnectionMetaData(ConnectionDelegate connection)
      throws JMSException
   {
      this.connection = connection;
   }

	// Public --------------------------------------------------------

   /**
    * Get the connection for this meta data 
    *      
    * @return the connection
    * @throws JMSException for any error
    */
   public ConnectionDelegate getConnection() throws JMSException
   {
      return connection;
   }

   // ConnectionMetaData implementation -----------------------------

	public int getJMSMajorVersion() throws JMSException
	{
		return 1;
	}

	public int getJMSMinorVersion() throws JMSException
	{
		return 1;
	}

	public String getJMSProviderName() throws JMSException
	{
      return "JBoss.org";
	}

	public String getJMSVersion() throws JMSException
	{
      return getJMSMajorVersion() + "." + getJMSMinorVersion();
	}

	public Enumeration getJMSXPropertyNames() throws JMSException
	{
       return connection.getJMSXPropertyNames();
	}

	public int getProviderMajorVersion() throws JMSException
	{
      return 4;
	}

	public int getProviderMinorVersion() throws JMSException
	{
		return 0;
	}

	public String getProviderVersion() throws JMSException
	{
      return getProviderMajorVersion() + "." + getProviderMinorVersion();
	}

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
