package common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/** Distributed filesystem paths.

 <p>
 Objects of type <code>Path</code> are used by all filesystem interfaces.
 Path objects are immutable.

 <p>
 The string representation of paths is a forward-slash-delimited sequence of
 path components (e.g., "/dir/file"). The root directory is represented as a
 single forward slash.

 <p>
 The colon (<code>':'</code>), semicolon (<code>';'</code>), forward slash
 (<code>'/'</code>), and space (<code>' '</code>) characters are not permitted
 within path components. The forward slash is the delimiter between path
 components, colon and semicolon are avoided for OS reasons, and space is
 avoided to allow string parsing using space-delimiters.
 */
public class Path implements Iterable<String>, Comparable<Path>, Serializable {
    /** The components of the path. */
    public List<String> components;

    /** Default Path constructor:
     - Creates a new path which represents the root directory.
     */
    public Path() {
        // Creates a new path representing the root directory
        this.components = Collections.singletonList("");
    }

    /** Appending Path constructor:
     - Creates a new path by appending a file name to an existing directory path.

     @param path The existing path representing a directory.
     @param component The new component representing a file name.
     @throws IllegalArgumentException If <code>component</code> is null, empty,
     or includes any characters that are not permitted.
     */
    public Path(Path path, String component) {
        // Appending Path constructor
        if (component == null || component.isEmpty() || component.matches(".*[:;\\/\\s].*")) {
            throw new IllegalArgumentException("Component contains invalid characters or is null/empty");
        }

        this.components = new ArrayList<>(path.components);
        this.components.add(component);
    }

    /** New Path constructor:
     - Creates a new Path from a String which starts with a forward slash
     and includes a sequence of forward-slash-delimited components.

     @param path The path string.
     @throws IllegalArgumentException If <code>path</code> is null, empty,
     or includes any characters that are not permitted.
     */
    public Path(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Invalid path string: path is null or empty");
        }

        // Normalize the path to deduplicate forward slashes
        String normalizedPath = path.replaceAll("/+", "/");

        // Ensures the path starts with a single forward slash
        if (!normalizedPath.startsWith("/")) {
            throw new IllegalArgumentException("Invalid path string: must start with '/'");
        }

        // Initializes the components list
        this.components = new ArrayList<>();

        // Checks if the path is the root and handles it accordingly
        if (normalizedPath.equals("/")) {
            this.components.add(""); // Represents the root directory
        } else {
            // Splits the path into components for non-root paths
            String[] parts = normalizedPath.substring(1).split("/");
            for (String part : parts) {
                if (!part.isEmpty() && !part.matches(".*[:;\\s].*")) {
                    this.components.add(part);
                }
            }
        }
    }

    /** Returns an iterator over the components of the path.

     <p>
     The iterator cannot be used to modify the path object - the
     <code>remove</code> method is not supported.

     @return The iterator.
     */
    @Override
    public Iterator<String> iterator() {
        // Iterator over the components of the path
        return components.iterator();
    }

    /** Lists the paths of all files in a directory tree on the local
     filesystem.

        @param directory The root directory of the directory tree.
        @return An array of relative paths, one for each file in the directory tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                does not refer to a directory.
     */
    public static Path[] list(File directory) throws FileNotFoundException {
        if (!directory.exists() || !directory.isDirectory()) {
            throw new FileNotFoundException("Root directory does not exist or is not a directory");
        }

        List<Path> paths = new ArrayList<>();
        // Assuming the root directory should not pass an initial "/" or "" to avoid "//" in paths
        // Instead, use a relative approach for the root directory
        listFiles(directory, paths, new Path("/")); // Pass root directory correctly
        return paths.toArray(new Path[0]);
    }

    /** Helper method to list files in a directory tree.

        @param directory The root directory of the directory tree.
        @param paths The list of paths to which to add the paths of files in the directory tree.
        @param parentPath The path of the parent directory of the directory tree.
     */
    private static void listFiles(File directory, List<Path> paths, Path parentPath) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                // For every file or directory found, construct the path from parent
                if (file.isDirectory()) {
                    // Recursively list files within the directory, adjust to not prepend extra slash
                    listFiles(file, paths, appendToPath(parentPath, file.getName()));
                } else {
                    // Add the file's path to the list, ensuring file paths don't start with "//"
                    paths.add(appendToPath(parentPath, file.getName()));
                }
            }
        }
    }

    /** Helper method to append a child name to a parent path.

        @param parentPath The parent path.
        @param childName The name of the child.
        @return The new path.
     */
    private static Path appendToPath(Path parentPath, String childName) {
        // If the parent path is root ("/"), append child directly without adding extra slash
        if (parentPath.toString().equals("/")) {
            return new Path("/" + childName);
        } else {
            // For non-root, append normally
            return new Path(parentPath.toString() + "/" + childName);
        }
    }

    /** Determines whether the path represents the root directory.

     @return <code>true</code> if the path does represent the root directory,
     and <code>false</code> if it does not.
     */
    public boolean isRoot() {
        return components.size() == 1 && components.get(0).isEmpty();
    }

    /** Returns the path to the parent of this path.

     @throws IllegalArgumentException If the path represents the root
     directory, and therefore has no parent.
     */
    public Path parent() {
        if (isRoot()) {
            return null;
        }

        List<String> parentComponents = new ArrayList<>(this.components);
        // Remove the last component to get the parent's components
        parentComponents.remove(parentComponents.size() - 1);

        // Edge case: If the parentComponents is empty after removal, it means this path was directly under root.
        if (parentComponents.isEmpty()) {
            return new Path("/"); // Directly return the root path.
        } else {
            // Reconstruct the parent path string, ensuring it starts with "/".
            String parentPathStr = "/" + String.join("/", parentComponents);
            return new Path(parentPathStr);
        }
    }


    /** Returns the last component in the path.

     @throws IllegalArgumentException If the path represents the root directory,
     and therefore has no last component.
     */
    public String last() {
        // Returns the last component in the path
        if (isRoot()) {
            throw new IllegalArgumentException("Root directory has no last component");
        }
        return components.get(components.size() - 1);
    }

    /** Determines if the given path is a subpath of this path.

     <p>
     The other path is a subpath of this path if it is a prefix of this path.
     Note that by this definition, each path is a subpath of itself.

     @param other The path to be tested.
     @return <code>true</code> If and only if the other path is a subpath of
     this path.
     */
    public boolean isSubpath(Path other) {
        // Determines if the given path is a subpath of this path
        // System.out.println("Checking subpath from Path: " + this.toString() + " Other: " + other.toString());
        if (other.components.size() < this.components.size()) {
            return false;
        }

        for (int i = 0; i < this.components.size(); i++) {
            if (!this.components.get(i).equals(other.components.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** Compares two paths lexicographically.

     <p>
     The comparison is based on the relative ordering of path components. The
     root directory is the highest component, and is followed by subdirectories
     in lexicographic order. The root directory is less than any subdirectory.

     @param other The other path to be compared.
     @return A negative integer, zero, or a positive integer as this path is
     less than, equal to, or greater than the other path.
     */
    @Override
    public int compareTo(Path other) {
        // Compares this path to another
        int minLength = Math.min(this.components.size(), other.components.size());
        for (int i = 0; i < minLength; i++) {
            int cmp = this.components.get(i).compareTo(other.components.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(this.components.size(), other.components.size());
    }


    /** Compares two paths for equality.

     <p>
     Two paths are equal if they have the same components.

     @param other The other path to be compared.
     @return <code>true</code> if the two paths are equal, and <code>false</code> if they are not.
     */
    @Override
    public boolean equals(Object other) {
        // Compares two paths for equality
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        Path path = (Path) other;
        return Objects.equals(components, path.components);
    }

    /** Returns the hash code of the path.

     @return The hash code of the path.
     */
    @Override
    public int hashCode() {
        return Objects.hash(components);
    }

    /** Returns the string representation of the path.

     @return The string representation of the path.
     */
    @Override
    public String toString() {
        return "/" + String.join("/", components);
    }
}
