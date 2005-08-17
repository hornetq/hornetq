/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.tools.jmx;

import javax.resource.spi.ManagedConnectionFactory;

public interface ManagedConnectionFactoryJMXWrapperMBean
{

   ManagedConnectionFactory getManagedConnectionFactory();
   ManagedConnectionFactory getMcfInstance();


   public void start() throws Exception;
   public void stop() throws Exception;

}
