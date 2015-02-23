/*
 * eXist Open Source Native XML Database
 * Copyright (C) 2001-2014 The eXist Project
 * http://exist-db.org
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.revisions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class Utils {

    final static int TAG_RESOURCE_TYPE_POS = 184;
    final static int TAG_RESOURCE_TYPE_LENGTH = 10;
    final static String TAG_RESOURCE_TYPE = "<"+Constants.EL_RESOURCE_TYPE+">";

    protected static String readResourceType(Path location) throws IOException {
        try (SeekableByteChannel ch = Files.newByteChannel(location, StandardOpenOption.READ)) {

            ch.position(TAG_RESOURCE_TYPE_POS);

            ByteBuffer bb = ByteBuffer.allocate(TAG_RESOURCE_TYPE_LENGTH);

            if (ch.read(bb) != TAG_RESOURCE_TYPE_LENGTH) return null;

            System.out.println(new String(bb.array()));

            bb.rewind();

            for (int i = 0; i < TAG_RESOURCE_TYPE.length(); i++) {
                if (bb.get() != TAG_RESOURCE_TYPE.charAt(i)) {
                    return null;
                }
            }

            return new String(bb.array(), bb.position(), 3);
        }
    }

    final static int TAG_DATA_HASH_POS = 200;
    final static String TAG_DATA_HASH = "<"+Constants.EL_DATA_HASH+">";
    final static int HASH_LENGTH = 128;
    final static int TAG_DATA_HASH_LENGTH = HASH_LENGTH + 1 + 6; //71;

    protected static String readHash(Path location) throws IOException {
        try (SeekableByteChannel ch = Files.newByteChannel(location,StandardOpenOption.READ)) {

            ch.position(TAG_DATA_HASH_POS);

            ByteBuffer bb = ByteBuffer.allocate(TAG_DATA_HASH_LENGTH);

            if (ch.read(bb) != TAG_DATA_HASH_LENGTH) return null;

            bb.rewind();

            for (int i = 0; i < TAG_DATA_HASH.length(); i++) {
                if (bb.get() != TAG_DATA_HASH.charAt(i)) {
                    return null;
                }
            }

            if (bb.get(bb.position() + HASH_LENGTH) != '<') return null;

            return new String(bb.array(), bb.position(), HASH_LENGTH);
        }
    }

    public static Path makeFolder(Path folder, String name) {

        return folder
                .resolve(name.substring(0, 4))
                .resolve(name.substring(4, 8))
                .resolve(name);
    }
}
