/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core.local;

import org.jboss.test.messaging.core.local.base.TopicTestBase;
import org.jboss.messaging.core.local.Topic;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class TopicTest extends TopicTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public TopicTest(String name)
   {
      super(name);
   }

   // ChannelTestBase overrides  ------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

     topic = new Topic("test");
   }

   public void tearDown() throws Exception
   {
      topic = null;
      super.tearDown();
   }

   public void crashChannel() throws Exception
   {
      // doesn't matter
   }

   public void recoverChannel() throws Exception
   {
      // doesn't matter
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
