/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.list;

import java.util.Comparator;

import org.jboss.jms.server.MessageReference;

/**
 * A comparator that implements standard message ordering
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class StandardMessageComparator
   implements Comparator
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Comparator implementation -------------------------------------

   public int compare(Object o1, Object o2)
   {
      try
      {
         MessageReference r1 = (MessageReference) o1;
         MessageReference r2 = (MessageReference) o2;
         int p1 = r1.getPriority();
         int p2 = r2.getPriority();
         if (p1 != p2) return p2-p1;
         String l1 = r1.getMessageID();
         String l2 = r2.getMessageID();
         return l1.compareTo(l2);
      }
      catch (Exception e)
      {
         throw new RuntimeException("Error during comparison", e);
      }
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
