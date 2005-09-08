/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.perf;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TextMessageMessageFactory extends AbstractMessageFactory
{
   public Message getMessage(Session sess, int size) throws JMSException
   {
      byte[] bytes = getBytes(size);
      String s = new String(bytes);
      TextMessage theMessage = sess.createTextMessage(s);
      return theMessage;
   }
}
