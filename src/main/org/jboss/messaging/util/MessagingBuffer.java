/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
	
	long getUnsignedInt();
	
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
	
	Object getUnderlyingBuffer();
	
}
