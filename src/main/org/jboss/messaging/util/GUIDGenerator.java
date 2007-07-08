/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.util;

import org.jboss.util.id.GUID;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>6 Jul 2007
 *
 * $Id: $
 *
 */
public class GUIDGenerator
{
	public static String generateGUID()
	{
		String guid = new GUID().toString();
		
		//We reverse the guid - this is because the JBoss GUID generates strings which are often the same up until
		//the last few characters, this means comparing them can be slow (when they're not equal) since many characters need
		//to be scanned
		
		int i;
		
		int len = guid.length();
		
		StringBuffer res = new StringBuffer(len);

		for (i = len -1; i >= 0; i--)
		{
			res.append(guid.charAt(i));
		}
		
		return res.toString();
	}
}
