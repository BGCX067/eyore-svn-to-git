package edu.colorado.eyore.common;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.logging.Logger;


public class JarClassLoadUtil {
	
	private static Logger logger = Logger.getLogger(JarClassLoadUtil.class.getName());

	/**
	 * Load a class from a jar file on the local file system
	 * 
	 * @param fullClassName
	 *            - qualified with package - e.g. com.foo.MyClass
	 * @param localJarFile
	 *            - file object for the jar file from which the class will be
	 *            loaded
	 * @return a reference to the loaded clas
	 */
	public static <T> Class<T> loadClassFromJar(String fullClassName, File localJarFile) {
		if (!localJarFile.exists()) {
			throw new RuntimeException("Can't find jar: " + localJarFile);
		}
		try {
			URLClassLoader ucl = URLClassLoader
					.newInstance(new URL[] { new URL("file", null, localJarFile
							.getAbsolutePath()) });

			return (Class<T>) ucl.loadClass(fullClassName);
		} catch (Exception e) {
			throw new RuntimeException("Could not load class " +
					fullClassName + " from jar " + localJarFile.getAbsolutePath()
					+ " - is the path name qualified with the package?" ,e);
		}
	}
	
	/**
	 * Loads & returns the classes found in a jar file that extend the specified
	 * superClass. 
	 * 
	 * @param <T>
	 * @param superClass
	 * @param localJarFile
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static List<Class> loadClassFromJar(Class superClass, File localJarFile)throws IOException{
		List<String> classNames = getAllClassNamesFromJar(localJarFile);		
		
		ArrayList<Class> loadedClasses = new ArrayList<Class>();
		
		for(String className : classNames){
			
			Class clazz = null;

			try{
				clazz = loadClassFromJar(className, localJarFile);
			}catch(Exception e){
				logger.severe("Failed loading class with name " + 
						className + " from jar " + localJarFile.getAbsolutePath());
				continue;
			}

			Class clazzSuperClass = null;
			while(! Object.class.equals(clazzSuperClass)){
				clazzSuperClass = clazz.getSuperclass();

				if(superClass.equals(clazzSuperClass)){
					loadedClasses.add(clazz);
					break;
				}
				clazzSuperClass = clazzSuperClass.getSuperclass();
			}
		}
		
		return loadedClasses;
	}

	/**
	 * Given a reference to a Class (e.g. from the loadClassFromJar method),
	 * instantiate the class using a zero-argument constructor (to keep things
	 * simple)
	 * 
	 * @param <T>
	 * @param clazz
	 *            the Class reference
	 */
	public static <T> T getInstanceFromClass(Class<T> clazz) {
		try {
			return clazz.getConstructor().newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate class " + clazz
					+ "make sure it has a zero-argument constructor", e);
		}
	}
	

	/**
	 * Returns a list of class names inside a jar archive.  The names are returned
	 * without the .class suffix so that they are the actual fully qualified class
	 * names.
	 * 
	 * @param pathToLocalJar
	 * @return
	 * @throws IOException
	 */
	public static List<String> getAllClassNamesFromJar(File pathToLocalJar)throws IOException{
		if(! pathToLocalJar.exists()){
			throw new IllegalStateException("Jar not found: " + pathToLocalJar.getAbsolutePath());
		}
		
		ArrayList<String> classNames = new ArrayList<String>();
		
		JarFile jar = new JarFile(pathToLocalJar);
		Enumeration<JarEntry> files = jar.entries();
		while(files.hasMoreElements()){
			String possibleClass = files.nextElement().getName();

			if(possibleClass.endsWith(".class")){
				String possibleClassName = possibleClass.replaceFirst(".class$", "")
						.replace("/", ".").replace("\\",".");
				classNames.add(possibleClassName);
			}
		}
		
		return classNames;
	}
	
	/*public static void main(String[] a)throws Exception{
		for(Class c :loadClassFromJar(JobSpecification.class, 
				new File("/Users/sbalough/Documents/workspace/eyore_svn/build/eyore/examplejobs/examplejob-echo.jar"))){
			System.err.println(c.getName());
		}
	}*/

}
