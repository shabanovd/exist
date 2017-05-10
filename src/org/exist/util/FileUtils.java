/*
 * FileUtils.java
 *
 * Created on December 10, 2007, 1:11 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.exist.util;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;

/**
 *
 * @author alex
 */
public class FileUtils
{
   
   // Why is this here?  Because we can't use generics because we're
   // still in the dark ages of Java 1.4
   
   static class FileRef {
      File file;
      FileRef next;
      FileRef(FileRef next,File file) {
         this.next = next;
         this.file = file;
      }
      FileRef(File file) {
         this.next = null;
         this.file = file;
      }
   }
   static class DeleteDir {
      FileRef current;
      boolean ok;
      DeleteDir(File dir) {
         current = new FileRef(dir);
         ok = true;
      }
      public boolean delete() {
         while (ok && current!=null) {
            final FileRef work = current;
            current.file.listFiles(new FileFilter() {
               public boolean accept(File file) {
                  if (file.isDirectory()) {
                     current = new FileRef(current,file);
                  } else {
                     ok = file.delete();
                  }
                  return false;
               }
            });
            if (current==work) {
               ok = current.file.delete();
               current = current.next;
            }
         }
         return ok;
      }
   }
   /** Creates a new instance of FileUtils */
   private FileUtils()
   {
   }
   
   public static boolean delete(File dir)
   {
      if (!dir.isDirectory()) {
         return dir.delete();
      }
      final DeleteDir doDelete = new DeleteDir(dir);
      return doDelete.delete();
      
   }
   
   /**
    * @param path a path or uri
    * @return the directory portion of a path by stripping the last '/' and
    * anything following, unless the path has no '/', in which case '.' is returned,
    * or ends with '/', in 
    * which case return the path unchanged.
    */
   public static String dirname (String path) {
       final int islash = path.lastIndexOf('/');
       if (islash >= 0 && islash < path.length() - 1)
           {return path.substring(0, islash);}
       else if (islash >= 0)
           {return path;}
       else
           {return ".";}
   }

   /**
    * @param path1
    * @param path2
    * @return path1 + path2, joined by a single file separator (or /, if a slash is already present).
    */
   public static String addPaths(String path1, String path2) {
       if (path1.endsWith("/") || path2.endsWith (File.separator)) {
           if (path2.startsWith("/") || path2.startsWith(File.separator)) {
               return path1 + path2.substring (1);
           } else {
               return path1 + path2;
           }
       } else {
           if (path2.startsWith("/") || path2.startsWith(File.separator)) {
               return path1 + path2;
           } else {
               return path1 + File.separatorChar + path2;
           }
       }
   }

    /**
     * Get just the filename part of the path
     *
     * @return The filename
     */
    public static String fileName(final Path path) {
        return path.getFileName().toString();
    }

    /**
     * Deletes a path from the filesystem
     *
     * If the path is a directory its contents
     * will be recursively deleted before it itself
     * is deleted.
     *
     * Note that removal of a directory is not an atomic-operation
     * and so if an error occurs during removal, some of the directories
     * descendants may have already been removed
     *
     * @throws IOException if an error occurs whilst removing a file or directory
     */
    public static void delete(final Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            Files.deleteIfExists(path);
        } else {
            Files.walkFileTree(path, deleteDirVisitor);
        }
    }

    private final static SimpleFileVisitor<Path> deleteDirVisitor = new DeleteDirVisitor();

    private static class DeleteDirVisitor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            if (exc != null) {
                throw exc;
            }

            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
        }
    }

  /**
   * Attempts to resolve the child
   * against the parent.
   *
   * If there is no parent, then the child
   * is resolved relative to the CWD
   *
   * @return The resolved path
   */
  public static Path resolve(final Optional<Path> parent, final String child) {
    return parent.map(p -> p.resolve(child)).orElse(Paths.get(child));
  }
}
