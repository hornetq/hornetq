/*
* JBoss, the OpenSource J2EE webOS
*
* Distributable under LGPL license.
* See terms of license at gnu.org.
*/
package org.jboss.jms.selector;

import java.util.HashMap;

/**
 * An interface describing a JMS selector expression parser.
 * 
 * @author Scott.Stark@jboss.org
 * @version $Revision$
 */
public interface ISelectorParser
{
   public Object parse(String selector, HashMap identifierMap) throws Exception;
   public Object parse(String selector, HashMap identifierMap, boolean trace) throws Exception;
}
