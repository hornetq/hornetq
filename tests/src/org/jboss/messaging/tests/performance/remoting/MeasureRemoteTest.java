/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.tests.performance.remoting;

import static org.jboss.messaging.core.remoting.TransportType.TCP;

import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.tests.integration.core.remoting.mina.TestSupport;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;

public class MeasureRemoteTest extends MeasureBase
{

   @Override
   protected LocationImpl getLocation()
   {
      return new LocationImpl(TCP, "localhost", TestSupport.PORT);
   }
   
   @Override
   protected ConfigurationImpl createConfiguration()
   {
      return ConfigurationHelper.newTCPConfiguration("localhost", TestSupport.PORT);
   }



}
