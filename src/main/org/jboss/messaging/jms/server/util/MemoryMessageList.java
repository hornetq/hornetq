/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.server.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;


import EDU.oswego.cs.dl.util.concurrent.FIFOReadWriteLock;
import EDU.oswego.cs.dl.util.concurrent.SyncSet;
import org.jboss.messaging.jms.server.MessageReference;

/**
 * An in memory implementation of the message list
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class MemoryMessageList
   implements MessageList
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The list */
   private SyncSet list;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MemoryMessageList()
   {
      Comparator comparator = new StandardMessageComparator();
      TreeSet set = new TreeSet(comparator);
      list = new SyncSet(set, new FIFOReadWriteLock());  
   }

   // Public --------------------------------------------------------

   // MessageList implementation ------------------------------------

   public void add(MessageReference message)
   {
      list.add(message);
   }
   
   public List browse(String selector)
      throws Exception
   {
      ArrayList result = new ArrayList(list.size());
      for (Iterator i = list.iterator(); i.hasNext();)
      {
         MessageReference reference = (MessageReference) i.next();
         result.add(reference.getMessage());
      }
      return result;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------

}
