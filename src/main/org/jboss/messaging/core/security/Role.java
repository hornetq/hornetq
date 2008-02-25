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
package org.jboss.messaging.core.security;

import java.io.Serializable;

/**
 * A role is used by the security store to define access rights and is configured on a connection factory or destination
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class Role implements Serializable
{
	public static final String WRITE_NAME="write";
	
	public static final String READ_NAME="read";
	
	public static final String CREATE_NAME="create";
		
	String name;
	
	private boolean read = false;
	
	private boolean write = false;
	
	private boolean create = false;

	public Role(String name)
	{
		this.name = name;
	}

	public Role(String name, boolean read, boolean write, boolean create)
	{
		this.name = name;
		this.read = read;
		this.write = write;
		this.create = create;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public boolean isRead()
	{
		return read;
	}

	public void setRead(boolean read)
	{
		this.read = read;
	}

	public boolean isWrite()
	{
		return write;
	}

	public void setWrite(boolean write)
	{
		this.write = write;
	}

	public boolean isCreate()
	{
		return create;
	}

	public void setCreate(boolean create)
	{
		this.create = create;
	}

	public boolean isCheckType(CheckType checkType)
	{
		return checkType.equals(CheckType.READ) ? read : checkType.equals(CheckType.WRITE) ? write : create;
	}

	public String toString()
	{
		return "Role {name=" + name + ";read=" + read + ";write=" + write + ";create=" + create + "}";
	}

	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Role role = (Role) o;

		if (create != role.create) return false;
		if (read != role.read) return false;
		if (write != role.write) return false;
		if (!name.equals(role.name)) return false;

		return true;
	}

	public int hashCode()
	{
		int result;
		result = name.hashCode();
		result = 31 * result + (read ? 1 : 0);
		result = 31 * result + (write ? 1 : 0);
		result = 31 * result + (create ? 1 : 0);
		return result;
	}
}
