/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import java.util.Hashtable;

import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class JNDITesterService implements JNDITesterServiceMBean
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // JNDITesterServiceMBean implementation --------------------------------------------------------

   public Object installAndUseJNDIEnvironment(Hashtable environment, String thingToLookUp)
      throws Exception
   {

      InitialContext ic = new InitialContext(environment);

      return ic.lookup(thingToLookUp);
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
