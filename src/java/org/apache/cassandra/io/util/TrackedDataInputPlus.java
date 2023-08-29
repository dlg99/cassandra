/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import net.nicoulaj.compilecommand.annotations.Inline;

/**
 * This class is to track bytes read from given DataInput
 */
public class TrackedDataInputPlus implements DataInputPlus, BytesReadTracker
{
    private long bytesRead;
    private final long limit;
    final DataInput source;

    /**
     * Create a TrackedDataInputPlus from given DataInput with no limit of bytes to read
     */
    public TrackedDataInputPlus(DataInput source)
    {
        this(source, -1);
    }

    /**
     * Create a TrackedDataInputPlus from given DataInput with limit of bytes to read. If limit is reached
     * {@link IOException} will be thrown when trying to read more bytes.
     */
    public TrackedDataInputPlus(DataInput source, long limit)
    {
        this.source = source;
        this.limit = limit;
    }

    public long getBytesRead()
    {
        return bytesRead;
    }

    /**
     * reset counter to @param count
     */
    public void reset(long count)
    {
        bytesRead = count;
    }

    public boolean readBoolean() throws IOException
    {
        checkCanRead(1);
        boolean bool = source.readBoolean();
        bytesRead += 1;
        return bool;
    }

    public byte readByte() throws IOException
    {
        checkCanRead(1);
        byte b = source.readByte();
        bytesRead += 1;
        return b;
    }

    public char readChar() throws IOException
    {
        checkCanRead(2);
        char c = source.readChar();
        bytesRead += 2;
        return c;
    }

    public double readDouble() throws IOException
    {
        checkCanRead(8);
        double d = source.readDouble();
        bytesRead += 8;
        return d;
    }

    public float readFloat() throws IOException
    {
        checkCanRead(4);
        float f = source.readFloat();
        bytesRead += 4;
        return f;
    }

    public void readFully(byte[] b, int off, int len) throws IOException
    {
        checkCanRead(len);
        source.readFully(b, off, len);
        bytesRead += len;
    }

    public void readFully(byte[] b) throws IOException
    {
        checkCanRead(b.length);
        source.readFully(b);
        bytesRead += b.length;
    }

    public int readInt() throws IOException
    {
        checkCanRead(4);
        int i = source.readInt();
        bytesRead += 4;
        return i;
    }

    public String readLine() throws IOException
    {
        // since this method is deprecated and cannot track bytes read
        // just throw exception
        throw new UnsupportedOperationException();
    }

    public long readLong() throws IOException
    {
        checkCanRead(8);
        long l = source.readLong();
        bytesRead += 8;
        return l;
    }

    public short readShort() throws IOException
    {
        checkCanRead(2);
        short s = source.readShort();
        bytesRead += 2;
        return s;
    }

    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }

    public int readUnsignedByte() throws IOException
    {
        checkCanRead(1);
        int i = source.readUnsignedByte();
        bytesRead += 1;
        return i;
    }

    public int readUnsignedShort() throws IOException
    {
        checkCanRead(2);
        int i = source.readUnsignedShort();
        bytesRead += 2;
        return i;
    }

    @Inline
    private void checkCanRead(int size) throws IOException
    {
        if (limit >= 0 && bytesRead + size > limit)
        {
            skipBytes((int) (limit - bytesRead));
            throw new EOFException("EOF after " + (limit - bytesRead) + " bytes out of " + size);
        }
    }

    public int skipBytes(int n) throws IOException
    {
        int skipped = source.skipBytes(limit < 0 ? n : (int) Math.min(limit - bytesRead, n));
        bytesRead += skipped;
        return skipped;
    }
}
