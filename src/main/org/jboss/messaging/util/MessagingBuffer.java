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

package org.jboss.messaging.util;

/**
 * 
 * A MessagingBuffer
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface MessagingBuffer
{
	void putByte(byte val);
	
	void putBytes(byte[] bytes);
	
	void putBytes(byte[] bytes, int offset, int length);
	
	void putInt(int val);
	
	void putInt(int pos, int val);
	
	void putLong(long val);
	
	void putShort(short val);
	
	void putDouble(double val);
	
	void putFloat(float val);
	
	void putBoolean(boolean val);
	
	void putChar(char val);
	
	void putNullableString(String val);
	
	void putString(String val);
			
	void putSimpleString(SimpleString val);
	
	void putNullableSimpleString(SimpleString val);
	
	void putUTF(String utf) throws Exception;
	
	byte getByte();
	
	short getUnsignedByte();
	
	void getBytes(byte[] bytes);
	
	void getBytes(byte[] bytes, int offset, int length);
	
	int getInt();
	
	long getLong();
	
	short getShort();
	
	int getUnsignedShort();
	
	double getDouble();
	
	float getFloat();
	
	boolean getBoolean();
	
	char getChar();
	
	String getString();
	
	String getNullableString();
	
	SimpleString getSimpleString();
	
	SimpleString getNullableSimpleString();
	
	String getUTF() throws Exception;
		
	byte[] array();
	
	int remaining();
	
	int capacity();
	
	int limit();
	
	void limit(int limit);
	
	void flip();
	
	void position(int position);
	
	int position();
	
	void rewind();
	
	MessagingBuffer slice();	
}
