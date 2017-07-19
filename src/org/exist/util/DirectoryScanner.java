package org.exist.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DirectoryScanner {

	
	private final static String extractBaseDir(String pattern) {
		int p = 0;
		char ch;
		for(int i = 0; i < pattern.length(); i++) {
			ch = pattern.charAt(i);
			if(ch == File.separatorChar || ch == ':') {
				p = i;
				continue;
			} else if(ch == '*' || ch == '?') {
				if(p > 0)
					{return pattern.substring(0, p + 1);}
			}
		}
		return null;
	}

	public static List<Path> scanDir_(String pattern) throws IOException {
		//TODO : why this test ? File should make it ! -pb
		pattern = pattern.replace('/', File.separatorChar).replace('\\', File.separatorChar);
		String baseDir = extractBaseDir(pattern);
		if (baseDir == null) {
			// Dizzzz ##### Why this dependancy?
			baseDir = System.getProperty("user.dir");
			pattern = baseDir + File.separator + pattern;
		}

		final Path base = Paths.get(baseDir).normalize();
		return scanDir_(base, pattern.substring(baseDir.length()));
	}

	public final static List<Path> scanDir_(final Path baseDir, String pattern) throws IOException {
		///TODO : why this test ? File should make it ! -pb
		pattern = pattern.replace('/', File.separatorChar).replace('\\', File.separatorChar);
		final List<Path> list = new ArrayList<>();
		scanDir_(list, baseDir, "", pattern);
		return list;
	}

	private final static void scanDir_(final List<Path> list, final Path dir, final String vpath, final String pattern) throws IOException {
		final List<Path> files = FileUtils.list(dir);
		for (final Path file : files) {
			final String name = vpath + FileUtils.fileName(file);
			if (Files.isDirectory(file) && matchStart(pattern, name)) {
				scanDir_(list, file, name + File.separator, pattern);
			} else if (match(pattern, name)) {
				list.add(file);
			}
		}
	}
	
	public final static File[] scanDir(String pattern) {
        //TODO : why this test ? File should make it ! -pb
		pattern = pattern.replace('/', File.separatorChar).replace('\\',File.separatorChar);
		String baseDir = extractBaseDir(pattern);
		if(baseDir == null) {
                        // Dizzzz ##### Why this dependancy?
			baseDir = System.getProperty("user.dir");
			pattern = baseDir + File.separator + pattern;
		}
		
		final File base = new File(baseDir);
		return scanDir(base, pattern.substring(baseDir.length()));
	}
	
	public final static File[] scanDir(File baseDir, String pattern) {
        ///TODO : why this test ? File should make it ! -pb
		pattern = pattern.replace('/', File.separatorChar).replace('\\',File.separatorChar);
		final ArrayList<File> list = new ArrayList<File>();
		scanDir(list, baseDir, "", pattern);
		final File[] files = new File[list.size()];
		return (File[])list.toArray(files);
	}
	
	private final static void scanDir(ArrayList<File> list, File dir, String vpath, String pattern) {
		final String files[] = dir.list();
		if (files == null) {
			return;
		}
		File file;
		String name;
		for(int i = 0; i < files.length; i++) {
			file = new File(dir, files[i]);
			name = vpath + files[i];
			if(file.isDirectory() && matchStart(pattern, name)) {
				scanDir(list, file, name + File.separator, pattern);
			} else if(match(pattern, name))
				{list.add(file);}
		}
	}
				
	public final static boolean match(String pattern, String name) {
		return SelectorUtils.matchPath(pattern, name);
	}

	public final static boolean matchStart(String pattern, String name) {
		return SelectorUtils.matchPatternStart(pattern, name);
	}
	
	public static void main(String args[]) {
		File files[] = scanDir("/home/*/xml/**/*.xml");
		for(int i = 0; i < files.length; i++)
			System.out.println(files[i].getAbsolutePath());
		
		files = scanDir("/does-not-exist/*.xml");
		for(int i = 0; i < files.length; i++)
			System.out.println(files[i].getAbsolutePath());
	}
}
				
