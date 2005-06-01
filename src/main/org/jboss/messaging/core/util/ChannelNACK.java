/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.messaging.core.State;

import java.util.Set;
import java.util.Collections;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ChannelNACK implements State
{
   // Constants -----------------------------------------------------

   public static final ChannelNACK instance = new ChannelNACK();

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   // State implementation ------------------------------------------

   public boolean isChannelNACK()
   {
      return true;
   }


   public int nackCount()
   {
      return 0;
   }


   public Set getNACK()
   {
      return Collections.EMPTY_SET;
   }


   public int ackCount()
   {
      return 0;
   }


   public Set getACK()
   {
      return Collections.EMPTY_SET;
   }


   public int size()
   {
      return 0;
   }


   public int nonCommittedCount()
   {
      return 0;
   }


   public Set getNonCommitted()
   {
      return Collections.EMPTY_SET;
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
