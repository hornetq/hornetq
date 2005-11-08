/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import javax.management.ObjectName;


/**
 * JDBCStateManager MBean interface
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Partially derived from org.jboss.mq.sm.jdbc.JDBCStateManagerMBean
 *
 */
public interface JDBCStateManagerMBean
{   
   
   ObjectName getConnectionManager();

   void setConnectionManager(ObjectName connectionManagerName);

   /**
    * Gets the sqlProperties.
    * @return Returns a Properties    */
   String getSqlProperties();

   /**
    * Sets the sqlProperties.
    * @param value The sqlProperties to set    */
   void setSqlProperties(String value);
   
}
