/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class Provider
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String name;
   private Properties jndiProperties;
   // Map <executorName - executorURL>
   private Map executorMap;

   // Constructors --------------------------------------------------

   public Provider(String name)
   {
      this.name = name;
      executorMap = new HashMap();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   public String getName()
   {
      return name;
   }

   public void setJNDIProperties(Properties p)
   {
      jndiProperties = p;
   }

   public Properties getJNDIProperties()
   {
      return jndiProperties;
   }

   public void addExecutor(String executorName, String url)
   {
      executorMap.put(executorName, url);
   }

   public String getExecutorURL(String executorName)
   {
      return (String)executorMap.get(executorName);
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
