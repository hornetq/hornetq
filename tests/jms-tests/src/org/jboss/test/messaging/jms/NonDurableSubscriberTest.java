/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.test.messaging.jms;

import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.naming.InitialContext;


/**
 * Non-durable subscriber tests.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class NonDurableSubscriberTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext ic;

   // Constructors --------------------------------------------------

   public NonDurableSubscriberTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------
  
   /**
    * Test introduced as a result of a TCK failure.
    */
   public void testNonDurableSubscriberOnNullTopic() throws Exception
   {
      TopicConnection conn = null;
      
      try
      {      
	      conn = cf.createTopicConnection();
	
	      TopicSession ts = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      try
	      {
	         ts.createSubscriber(null);
	         fail("this should fail");
	      }
	      catch(javax.jms.InvalidDestinationException e)
	      {
	         // OK
	      }
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }

   /**
    * Test introduced as a result of a TCK failure.
    */
   public void testNonDurableSubscriberInvalidUnsubscribe() throws Exception
   {
      TopicConnection conn = null;
      
      try
      {	      
	      conn = cf.createTopicConnection();
	      conn.setClientID("sofiavergara");
	
	      TopicSession ts = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
	
	      try
	      {
	         ts.unsubscribe("invalid-subscription-name");
	         fail("this should fail");
	      }
	      catch(javax.jms.InvalidDestinationException e)
	      {
	         // OK
	      }
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
