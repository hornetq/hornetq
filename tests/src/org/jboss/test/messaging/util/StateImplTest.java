/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.messaging.core.util.StateImpl;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class StateImplTest extends MutableStateTest
{

   // Constructors --------------------------------------------------

   public StateImplTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      state = new StateImpl();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
      state = null;
      
   }

   //
   // It also runs MutableStateTest's tests.
   //

}
