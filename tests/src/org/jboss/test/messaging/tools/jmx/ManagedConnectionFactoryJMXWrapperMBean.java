/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.tools.jmx;

import javax.resource.spi.ManagedConnectionFactory;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface ManagedConnectionFactoryJMXWrapperMBean
{

   ManagedConnectionFactory getManagedConnectionFactory();
   ManagedConnectionFactory getMcfInstance();

   void start() throws Exception;
   void stop() throws Exception;

}
