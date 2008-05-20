/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.tests.integration.core.remoting.mina.timing;

import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;


/** This test was added to compare InVM calls against MINA calls */
public class MeasureInVMTest extends MeasureBase
{

   @Override
   protected LocationImpl getLocation()
   {
      return new LocationImpl(0);
      
   }

   protected ConfigurationImpl createConfiguration()
   {
      return ConfigurationHelper.newInVMConfig();
   }

   

}
