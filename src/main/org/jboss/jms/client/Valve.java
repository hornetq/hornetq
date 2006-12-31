/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

/**
 * An interface that controls the permeability of implementing objects.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface Valve
{
   /**
    * "Closes" the valve. The implementing object becomes impermeable. Did't go for "close()" to
    * avoid name conflicts with Closeable.close().
    */
   void closeValve() throws Exception;

   /**
    * "Opens" the valve. The implementing object becomes permeable. Didn't go for "open()" for
    * symmetry reasons.
    */
   void openValve() throws Exception;

   boolean isValveOpen();

   /**
    * @return the number of threads currenty "penetrating" the open valve. Shold be 0 if the valve
    *         is closed.
    */
   int getActiveThreadsCount();

}
