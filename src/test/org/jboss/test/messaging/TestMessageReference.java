/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging;

import java.util.Comparator;

import org.jboss.messaging.interfaces.MessageAddress;
import org.jboss.messaging.interfaces.MessageReference;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedLong;

/**
 * A simple implementation of a message reference
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public class TestMessageReference implements MessageReference
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   Long messageID;
   
   // Static --------------------------------------------------------

   private static final SynchronizedLong nextMessageID = new SynchronizedLong(0);

   // Constructors --------------------------------------------------
   
   public TestMessageReference()
   {
      messageID = new Long(nextMessageID.increment());
   }
   
   // Public --------------------------------------------------------
   
   // MessageReference implementation -------------------------------

   public Comparable getMessageID()
   {
      return messageID;
   }

   public void release()
   {
   }
   
   public MessageAddress getMessageAddress()
   {
      return null;
   }

   public int getMessagePriority()
   {
      return 0;
   }

   public boolean isGuaranteed()
   {
      return false;
   }
   
   // Protected -----------------------------------------------------
   
   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------

   public static class TestMessageReferenceComparator implements Comparator
   {
      public int compare(Object o1, Object o2)
      {
         Comparable m1 = ((TestMessageReference) o1).getMessageID();
         Comparable m2 = ((TestMessageReference) o2).getMessageID();
         return m1.compareTo(m2);
      }
   }
}
