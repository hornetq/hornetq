/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MapMessageMessageFactory extends AbstractMessageFactory
{
   private static final long serialVersionUID = 4180086702224773753L;

   public Message getMessage(Session sess, int size) throws JMSException
   {
      MapMessage theMessage = sess.createMapMessage();
      theMessage.setBytes("b", getBytes(size));
      return theMessage;
   }
}

