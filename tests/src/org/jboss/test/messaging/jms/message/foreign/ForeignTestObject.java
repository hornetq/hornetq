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
package org.jboss.test.messaging.jms.message.foreign;

import java.io.Serializable;

/**
 * A Simple Serializable Object
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class ForeignTestObject implements Serializable
{
	private static final long serialVersionUID = -7503042537789321104L;

	private String s1;
	private double d1;

	public ForeignTestObject(String s, double d)
	{
		s1 = s;
		d1 = d;
	}

	public double getD1()
	{
		return d1;
	}

	public void setD1(double d1)
	{
		this.d1 = d1;
	}

	public String getS1()
	{
		return s1;
	}

	public void setS1(String s1)
	{
		this.s1 = s1;
	}

	public boolean equals(Object o)
	{
		if(o instanceof ForeignTestObject)
		{
			ForeignTestObject to = (ForeignTestObject)o;

			return (s1.equals(to.getS1()) && d1 == to.getD1());
		}
		return super.equals(o);
	}

}
