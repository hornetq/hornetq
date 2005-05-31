/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.local.SingleOutputChannelSupport;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class SingleOutputChannelSupportTest extends TransactionalChannelSupportTest
{
   // Attributes ----------------------------------------------------

   SingleOutputChannelSupport singleOutputChannel;

   // Constructors --------------------------------------------------

   public SingleOutputChannelSupportTest(String name)
   {
      super(name);
   }

   public void setUp() throws Exception
   {
      singleOutputChannel = (SingleOutputChannelSupport)channel;
      super.setUp();
   }

   public void tearDown()throws Exception
   {
      singleOutputChannel = null;
      super.tearDown();
   }

   //
   //
   //

   public void testGetOutputID()
   {
      if (skip()) { return; }

      assertEquals(receiverOne.getReceiverID(), singleOutputChannel.getOutputID());
   }

   private boolean skip()
   {
      return singleOutputChannel == null;
   }

}