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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 */
public class Commits implements Iterable<CommitReader> {

    RCSHolder holder;

    Commits(RCSHolder holder) {
        this.holder = holder;
    }

    @Override
    public Iterator<CommitReader> iterator() {
        return new CommitsIterator();
    }

    class CommitsIterator implements Iterator<CommitReader> {

        ArrayList<Path> months;

        CommitsIterator() {
            Path folder = holder.commitLogsFolder;

            months = new ArrayList<>();

            try (DirectoryStream<Path> paths = Files.newDirectoryStream(folder)) {
                for (Path path : paths) {
                    months.add(path);
                }
            } catch (IOException e) {
                throw new RuntimeException("can't get list of months", e);
            }

            Collections.sort(months, new Comparator<Path>() {
                @Override
                public int compare(Path o1, Path o2) {
                    return o1.getFileName().compareTo(o2.getFileName());
                }
            });
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public CommitReader next() {
            return null;
        }

        @Override
        public void remove() {
        }
    }
}
