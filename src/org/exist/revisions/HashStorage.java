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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.exist.dom.BinaryDocument;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static java.nio.file.Files.createDirectories;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class HashStorage {

    Path hashesFolder;
    Path tmpFolder;

    protected HashStorage(Path hashesFolder, Path tmpFolder) {
        this.hashesFolder = hashesFolder;
        this.tmpFolder = tmpFolder;
    }

    public String put(InputStream is) throws IOException {

        MessageDigest digest = messageDigest();

        Path dataFile = Files.createTempFile(tmpFolder, "hashing", "data");

        try (OutputStream fileStream = Files.newOutputStream(dataFile)) {

            DigestOutputStream digestStream = new DigestOutputStream(fileStream, digest);

            IOUtils.copy(is, digestStream);
        }

        String hash = digestHex(digest);

        //check hash storage
        Path hashPath = Utils.makeFolder(hashesFolder, hash);
        if (Files.notExists(hashPath)) {
            createDirectories(hashPath.getParent());
            Files.move(dataFile, hashPath);
        } else {
            FileUtils.deleteQuietly(dataFile.toFile());
            //if fail it will be clean up next restart or it possible detect old files by last access time
        }

        return hash;
    }

    public InputStream get(String hash) throws IOException {
        Path hashPath = Utils.makeFolder(hashesFolder, hash);

        return Files.newInputStream(hashPath);
    }

    private String digestHex(MessageDigest digest) {
        return Hex.encodeHexString(digest.digest());
    }

    private MessageDigest messageDigest() throws IOException {
        try {
            return MessageDigest.getInstance(MessageDigestAlgorithms.SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }
}
