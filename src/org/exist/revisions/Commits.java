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
import java.util.*;

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

        LinkedList<Path> months;
        LinkedList<Path> commits;

        CommitReader next;

        CommitsIterator() {
            Path folder = holder.commitLogsFolder;

            months = new LinkedList<>();

            try (DirectoryStream<Path> paths = Files.newDirectoryStream(folder)) {
                for (Path path : paths) {
                    if (Files.isDirectory(path)) {
                        String name = path.getFileName().toString();
                        //simple check
                        if (name.length() == 7 && name.charAt(4) == '-') {
                            months.add(path);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("can't get list of months", e);
            }

            Collections.sort(months, (o1, o2) -> o1.getFileName().compareTo(o2.getFileName()));

            prepareNext();
        }

        private synchronized void prepareNext() {
            next = null;

            while (next == null) {
                if (commits == null || commits.isEmpty()) {
                    if (months.isEmpty()) return;

                    Path folder = months.pollFirst();

                    commits = new LinkedList<>();

                    try (DirectoryStream<Path> paths = Files.newDirectoryStream(folder)) {
                        for (Path path : paths) {
                            commits.add(path);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("can't get list of months", e);
                    }

                    Collections.sort(commits, (o1, o2) -> o1.getFileName().compareTo(o2.getFileName()));
                }

                Path commit = commits.pollFirst();
                try {
                    next = new CommitLoaded(holder, commit);
                } catch (Exception e) {
                    e.printStackTrace();
                    RCSManager.LOG.info(e.getMessage()+" '"+commit.toAbsolutePath().toString()+"'");
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public CommitReader next() {
            CommitReader current = next;

            prepareNext();

            return current;
        }

        @Override
        public void remove() {
            throw new IllegalAccessError("not allow");
        }
    }
}
