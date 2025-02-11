/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.codeAnalysis;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.codeAnalysis.decode.CompiledField;
import org.apache.geode.codeAnalysis.decode.CompiledMethod;
import org.apache.geode.internal.serialization.KnownVersion;

public class CompiledClassUtils {

  static Set<String> allowedDataSerializerMethods;

  static {
    allowedDataSerializerMethods = new HashSet<>();
    KnownVersion.getAllVersions().iterator().forEachRemaining((version) -> {
      allowedDataSerializerMethods.add("toDataPre_" + version.getMethodSuffix());
      allowedDataSerializerMethods.add("fromDataPre_" + version.getMethodSuffix());
    });
  }

  /**
   * Parse the given class files and return a map of name->Dclass. Any IO exceptions are consumed by
   * this method and written to stderr.
   *
   * @return the parsed classes
   */
  public static Map<String, CompiledClass> parseClassFiles(List<File> classFiles) {
    Map<String, CompiledClass> result = new HashMap<>();

    for (File file : classFiles) {
      try {
        CompiledClass parsed = CompiledClass.getInstance(file);
        if (!parsed.isInterface()) {
          result.put(parsed.fullyQualifiedName(), parsed);
        }
      } catch (IOException e) {
        System.err.println("Exception while parsing " + file.getName() + ": " + e.getMessage());
      }
    }
    return result;
  }

  /**
   * Parse the files in the given jar file and return a map of name->CompiledClass. Any IO
   * exceptions are consumed by this method and written to stderr.
   *
   * @param jar the jar file holding classes
   */
  public static Map<String, CompiledClass> parseClassFilesInJar(File jar) {
    Map<String, CompiledClass> result = new HashMap<>();
    try (JarFile jarfile = new JarFile(jar)) {
      for (Enumeration<JarEntry> entries = jarfile.entries(); entries.hasMoreElements();) {
        JarEntry entry = entries.nextElement();
        if (entry.getName().endsWith(".class")) {
          try {
            CompiledClass parsed = CompiledClass.getInstance(jarfile.getInputStream(entry));
            if (!parsed.isInterface()) {
              result.put(parsed.fullyQualifiedName(), parsed);
            }
          } catch (IOException e) {
            System.err
                .println("Exception while parsing " + entry.getName() + ": " + e.getMessage());
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Error opening jar file:");
      e.printStackTrace(System.err);
    }
    return result;
  }

  /**
   * Parse the files in the given jar file and return a map of name->CompiledClass. Any IO
   * exceptions are consumed by this method and written to stderr.
   */
  public static Map<String, CompiledClass> parseClassFilesInDir(File buildDir) {
    Map<String, CompiledClass> result = new HashMap<>();
    for (File entry : buildDir.listFiles()) {
      if (entry.isDirectory()) {
        result.putAll(parseClassFilesInDir(entry));
      } else if (entry.getName().endsWith(".class")) {
        try (FileInputStream fis = new FileInputStream(entry)) {
          CompiledClass parsed = CompiledClass.getInstance(fis);
          if (!parsed.isInterface()) {
            result.put(parsed.fullyQualifiedName(), parsed);
          }
        } catch (IOException e) {
          System.err.println("Exception while parsing " + entry.getName() + ": " + e.getMessage());
        }
      }
    }
    return result;
  }

  /**
   * returns a collection of all of the .class files in the given list of files and directories.
   *
   * @param filenames a list of the files and directories to examine
   * @param recursive whether to recurse into subdirectories
   * @return a sorted list of the .class files found
   */
  public static List<File> findClassFiles(String parentPath, String[] filenames,
      boolean recursive) {
    // Grab classes and Expand directory names found in list
    List<File> classFiles = new ArrayList<>();
    for (final String filename : filenames) {
      File f = new File(parentPath + filename);
      String n = f.getAbsolutePath();
      if (!f.exists()) {
        System.err.println("File " + n + " does not exist - skipping");
        continue;
      }
      if (f.isFile() && f.getName().endsWith(".class")) {
        classFiles.add(f);
        continue;
      }
      if (f.isDirectory() && recursive) {
        classFiles.addAll(findClassFiles(f.getAbsolutePath() + "/", f.list(), true));
      }
    }
    Collections.sort(classFiles);
    return classFiles;
  }

  public static List<ClassAndMethodDetails> loadClassesAndMethods(File file) throws IOException {
    List<ClassAndMethodDetails> result = new LinkedList<>();
    FileReader fr = new FileReader(file);
    LineNumberReader in = new LineNumberReader(fr);
    ClassAndMethodDetails cam;
    while ((cam = ClassAndMethodDetails.create(in)) != null) {
      result.add(cam);
    }
    fr.close();
    return result;
  }

  public static String diffSortedClassesAndMethods(List<ClassAndMethodDetails> goldRecord,
      List<ClassAndMethods> toDatas) throws IOException {

    StringBuilder newClassesSb = new StringBuilder(10000);
    StringBuilder changedClassesSb = new StringBuilder(10000);
    newClassesSb.append("New or moved classes----------------------------------------\n");
    int newBase = newClassesSb.length();
    changedClassesSb.append("Modified classes--------------------------------------------\n");
    int changedBase = changedClassesSb.length();

    Iterator<ClassAndMethods> it = toDatas.iterator();
    ClassAndMethods newclass = null;

    for (ClassAndMethodDetails gold : goldRecord) {
      if (newclass == null) {
        if (!it.hasNext()) {
          changedClassesSb.append(gold).append(": deleted or moved\n");
          continue;
        }
        newclass = it.next();
      }
      int comparison = -1;
      while (newclass != null
          && (comparison = gold.className.compareTo(newclass.dclass.fullyQualifiedName())) > 0) {
        newClassesSb.append(newclass).append("\n");
        if (it.hasNext()) {
          newclass = it.next();
        } else {
          newclass = null;
        }
      }
      if (comparison == 0) {
        ClassAndMethods nc = newclass;
        newclass = null;
        if (gold.methods.size() != nc.numMethods()) {
          changedClassesSb.append(nc).append(": method count (expected " + gold.methods.size()
              + " but found " + nc.numMethods() + ")\n");
          continue;
        }
        boolean comma = false;
        for (Map.Entry<String, CompiledMethod> entry : nc.methods.entrySet()) {
          CompiledMethod method = entry.getValue();
          String methodName = method.name();
          if (!methodName.equals("toData") && !methodName.equals("fromData")
              && !allowedDataSerializerMethods.contains(methodName)) {
            if (comma) {
              changedClassesSb.append(", and ");
            } else {
              changedClassesSb.append(nc).append(":  ");
              comma = true;
            }
            changedClassesSb.append(methodName)
                .append(" is not a valid method name - doesn't match any Version");
            continue;
          }
          Integer goldCode = gold.methods.get(methodName);
          if (goldCode == null) {
            if (comma) {
              changedClassesSb.append(", and ");
            } else {
              changedClassesSb.append(nc).append(":  ");
              comma = true;
            }
            changedClassesSb.append(methodName).append(" was added");
            continue; // only report one diff per class
          }
          if (goldCode != method.getCode().code.length) {
            if (comma) {
              changedClassesSb.append(", and ");
            } else {
              changedClassesSb.append(nc).append(":  ");
              comma = true;
            }
            changedClassesSb.append(methodName)
                .append(" (len=" + method.getCode().code.length + ",expected=" + goldCode + ")");
            continue;
          }
        }
        for (Map.Entry<String, Integer> entry : gold.methods.entrySet()) {
          if (!nc.methods.containsKey(entry.getKey())) {
            if (comma) {
              changedClassesSb.append(", and ");
            } else {
              changedClassesSb.append(nc).append(":  ");
            }
            changedClassesSb.append(entry.getKey()).append(" is missing");
          }
        }
        if (comma) {
          changedClassesSb.append("\n");
        }
      }
    }
    while (it.hasNext()) {
      newclass = it.next();
      newClassesSb.append(newclass).append(": new class\n");
    }
    String result = "";
    if (newClassesSb.length() > newBase) {
      if (changedClassesSb.length() > changedBase) {
        newClassesSb.append("\n");
        newClassesSb.append(changedClassesSb);
      }
      result = newClassesSb.toString();
    } else if (changedClassesSb.length() > changedBase) {
      result = changedClassesSb.toString();
    }
    return result;
  }

  public static void storeClassesAndMethods(List<ClassAndMethods> cams, File file)
      throws IOException {
    FileWriter fw = new FileWriter(file);
    BufferedWriter out = new BufferedWriter(fw);
    for (ClassAndMethods entry : cams) {
      out.append(ClassAndMethodDetails.convertForStoring(entry));
      out.newLine();
    }
    out.flush();
    out.close();
  }

  public static List<ClassAndVariableDetails> loadClassesAndVariables(InputStream stream)
      throws IOException {
    List<ClassAndVariableDetails> result = new LinkedList<>();
    BufferedReader in = new BufferedReader(new InputStreamReader(stream));
    String line;
    while ((line = in.readLine()) != null) {
      line = line.trim();
      if (isBlank(line) || line.startsWith("#") || line.startsWith("//")) {
        // comment line
        continue;
      }
      result.add(new ClassAndVariableDetails(line));
    }
    return result;
  }

  public static String diffSortedClassesAndVariables(List<ClassAndVariableDetails> goldRecord,
      List<ClassAndVariables> cavs) {

    StringBuilder newClassesSb = new StringBuilder(10000);
    StringBuilder changedClassesSb = new StringBuilder(10000);
    newClassesSb.append("New or moved classes----------------------------------------\n");
    int newBase = newClassesSb.length();
    changedClassesSb.append("Modified classes--------------------------------------------\n");
    int changedBase = changedClassesSb.length();

    Iterator<ClassAndVariables> it = cavs.iterator();
    ClassAndVariables newclass = null;

    List<String> added = new ArrayList<>();
    List<String> removed = new ArrayList<>();
    List<String> changed = new ArrayList<>();

    for (ClassAndVariableDetails gold : goldRecord) {
      added.clear();
      removed.clear();
      changed.clear();
      if (newclass == null) {
        if (!it.hasNext()) {
          changedClassesSb.append(gold).append(": deleted or moved\n");
          continue;
        }
        newclass = it.next();
      }
      int comparison = -1;
      while (newclass != null
          && (comparison = gold.className.compareTo(newclass.dclass.fullyQualifiedName())) > 0) {
        newClassesSb.append(ClassAndVariableDetails.convertForStoring(newclass)).append("\n");
        newclass = null;
        if (it.hasNext()) {
          newclass = it.next();
        }
      }
      if (comparison == 0) {
        ClassAndVariables nc = newclass;
        newclass = null;
        for (Map.Entry<String, CompiledField> entry : nc.variables.entrySet()) {
          CompiledField field = entry.getValue();
          String name = entry.getKey();
          String type = gold.variables.get(name);
          if (type == null) {
            added.add(name);
            continue; // only report one diff per class
          }
          String newType = field.descriptor();
          if (!newType.equals(type)) {
            changed.add(name + " type changed to " + newType);
            continue;
          }
        }
        for (Map.Entry<String, String> entry : gold.variables.entrySet()) {
          if (!nc.variables.containsKey(entry.getKey())) {
            removed.add(entry.getKey());
          }
        }
        if (!(added.isEmpty() && removed.isEmpty() && changed.isEmpty())) {
          changedClassesSb.append(nc).append('\n');
        }
        if (!added.isEmpty()) {
          changedClassesSb.append("\t\t added fields: ").append(added).append('\n');
        }
        if (!changed.isEmpty()) {
          changedClassesSb.append("\t\t changed fields: ").append(changed).append('\n');
        }
        if (gold.hasSerialVersionUID) {
          if (nc.hasSerialVersionUID) {
            if (!Long.valueOf(gold.serialVersionUID).equals(nc.serialVersionUID)) {
              changedClassesSb.append("\t\t " + nc.dclass.fullyQualifiedName()
                  + " serialVersionUID was changed from " + gold.serialVersionUID + " to "
                  + nc.serialVersionUID
                  + " this may break client/server compatibility as well as server/server compatibility \n");
            }
          } else {
            changedClassesSb.append("\t\t " + nc.dclass.fullyQualifiedName()
                + " serialVersionUID was removed, this may break client/server compatibility as well as server/server compatibility \n");
          }
        } else {
          if (nc.hasSerialVersionUID) {
            changedClassesSb.append(
                "\t\t " + nc.dclass.fullyQualifiedName() + " serialVersionUID was added \n");
          }
        }
      }
    }
    while (it.hasNext()) {
      newclass = it.next();
      newClassesSb.append(ClassAndVariableDetails.convertForStoring(newclass))
          .append(": new class\n");
    }
    String result = "";
    if (newClassesSb.length() > newBase) {
      if (changedClassesSb.length() > changedBase) {
        newClassesSb.append("\n");
        newClassesSb.append(changedClassesSb);
      }
      result = newClassesSb.toString();
    } else if (changedClassesSb.length() > changedBase) {
      result = changedClassesSb.toString();
    }
    return result;
  }

  public static void storeClassesAndVariables(List<ClassAndVariables> cams, File file)
      throws IOException {
    FileWriter fw = new FileWriter(file);
    BufferedWriter out = new BufferedWriter(fw);
    for (ClassAndVariables entry : cams) {
      out.append(ClassAndVariableDetails.convertForStoring(entry));
      out.newLine();
    }
    out.flush();
    out.close();
  }
}
