/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.facade;

import java.util.Enumeration;
import java.util.List;
import java.util.ListIterator;

/**
 * A browser enumeration.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
class BrowserEnumeration implements Enumeration
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The list iterator */
   private ListIterator iterator;

	// Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Create a new BrowserEnumeration.
    * 
    * @param list the list.
    */
   public BrowserEnumeration(List list)
   {
      iterator = list.listIterator();
   }

   // Enumeration implementation ------------------------------------

   public boolean hasMoreElements()
   {
      return iterator.hasNext();
   }

   public Object nextElement()
   {
      return iterator.next();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
