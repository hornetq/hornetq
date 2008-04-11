/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import java.util.Hashtable;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface JNDITesterServiceMBean
{
   Object installAndUseJNDIEnvironment(Hashtable environment, String thingToLookUp)
      throws Exception;
}
