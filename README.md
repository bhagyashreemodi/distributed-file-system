### Limitations & Failure Handling:
1. Network Failures: The code does not explicitly handle network failures during communication between the naming server and storage servers, or when storage servers communicate among themselves (e.g., during file replication). In a real-world scenario, network partitions or intermittent connectivity could lead to inconsistencies or data loss.
2. Storage Server Crashes: If a storage server crashes or goes offline, the system's ability to recover or continue operations without data loss is not clearly addressed. The system design does not explicitly detail strategies for detecting server failures, reassigning server responsibilities, or recovering lost data from replicas.
3. Scalability Limitations: The system design and code structure may face scalability issues as the number of files or storage servers increases. For example, maintaining a global list of files and servers on the naming server could become a bottleneck. Techniques like sharding or distributed hash tables (DHTs) might be needed for better scalability.

This file details the contents of the initial Lab 3 code repository and how to use it.

### Getting started

The codebase for this lab is independent of the previous labs.  If you used Java for the previous labs,
the look and feel of the test suite should look familiar, and your environment shouldn't need any changes.
If you used a different programming language for previous labs, you'll need to make sure you have a JDK
installed on your machine, since the test suite for this lab is written entirely in Java.  You can write
your DFS components in whatever (suitable) language you desire, but it will be launched by the Java test
code, so you may need to tweak your IDE to make it work.


### Initial repository contents

The top-level directory (called `lab3` here) of the initial starter-code repository includes multiple 
important components:
* This `README.md` file
* The Lab 3 `Makefile`, described in detail later
* The `gson-2.8.6.jar` JSON library used by the test code
* The `test` directory that contains the Lab 3 auto-grader, which you should not modify (with one exception)
* The `API` directory that includes a detailed specification of the commands that your naming and storage servers will need to support
* The `common` directory with wrappers for the JSON library that are used by the test code and available for your use

We'll go through all of these components one by one, but a high-level summary of the first level of contents visually looks like this:
```
\---lab3
        +---API
        +---common
        +---test
        |   +---common
        |   +---naming
        |   +---storage
        |   +---util
        |   +---Lab3CheckpointTests.java
        |   +---Lab3FinalTests.java
        |   \---ServerCommands.java
        +---gson-2.8.6.jar
        +---Makefile
        \---README.md
```
The details of each of these will hopefully become clear after reading the rest of this file.


### Building your DFS Components

The vast majority of your work in this lab will comprise two additional directories in your code repo. You'll
create directories called `naming` and `storage` within the Lab 3 repo (next to API and test). Using whatever
language you want, you'll build your Naming Server in `naming` and your Storage Server in `storage`.  Since all
of our servers and test code interact using REST APIs and JSON messages, there's quite a bit of flexibility
in your design.  If there is any code that is used by both the naming and storage servers, you can put it in
the `common` directory (what's already there is used by various parts of the test code).  For example, you'll
need to create a data structure to represent file paths in the file system, and it probably makes the most sense
to use the same implementation for both of your servers. Your naming and storage servers will technically be 
independent of each other (the test code has a naming server to test with your storage server and vice versa),
but it would be nice if they worked together, so you could have a complete implementation of your own. You are 
definitely welcome (and encouraged) to use the partial implementations in the test code to guide your initial
design, so it helps to understand the test suite.


### DFS API Specification

The `API` folder in the starter repository includes four markdown files that provide a detailed specification
for the various commands that must be supported on each of the interfaces between components of the DFS that
you are building. This specification serves as a guide to help you through the process of designing your RESTful
naming and storage server implementations. Each interface specification describes the commands send between
corresponding parties, the JSON formats used for sending arguments and return values, and the possible error
scenarios that must be addressed by your design.  However, this is only a summary, and a few details are 
explained in much more detail in the comments in the test code itself.


### Support for JSON Messaging

The `common` folder in the starter repository includes a variety of Java classes that represent the various 
message formats for sending arguments and return values between DFS components and the test code.  The `gson`
library (provided in `gson-2.8.6.jar`) provides a variety of utilities for translating between JSON-formatted
strings and these Java objects.  There are many examples of the use of the `gson` library in the test code, so
you should explore how that can be used.  If you're not building your DFS in Java, many other languages have
build-in support or similar packages for mapping JSON into native data structures, so you probably don't need
to use a third-party library like `gson`.


### Understanding the Test Suite

The test suite for Lab 3 is built entirely in Java and includes multiple sub-packages in the `test` package. The
test suite includes generic capabilities in `test.util` along with DFS-specific tests for the naming and storage
servers in the `test.naming` and `test.storage` packages (which share a bit of code in `test.common`).  All of these
tests are orchestrated by the main functions in the `test/Lab3CheckpointTests.java` and `test/Lab3FinalTests.java`
files.  The last file in the `test` package is `test/ServerCommands.java`, which is a very simple class that performs
a very important function; it defines three strings that encompass command-line instructions to launch your naming
and storage servers as generic processes within the Java virtual machine, regardless of what language they are 
built in. We'll come back to this file later.


### Testing your DFS Implementation

Once you're at the point where you want to run any of the provided tests, you can use the provided `make` rules or
simply execute the corresponding java commands. The `make checkpoint` and `make final` rules will compile and run the 
`test.Lab3CheckpointTests` and `test.Lab3FinalTests` main functions, respectively.  If you build your DFS components
in a language other than Java, you may need to modify the `Makefile` to include additional compilation steps under the
`make build` rule; if you're using something like Python or Go, you may not need to change anything in the `Makefile`.

During the development process, you are certainly welcome to modify the test code to run only a subset of the tests,
either by commenting out individual tests from the `Lab3CheckpointTests.java` and `Lab3FinalTests.java` files or by
creating alternate main files to drive your development.  Just remember that any modifications that you make to the
`test` package will not be reflected in the auto-grader, as we will restore the original `test` package from the starter
code repository when running the tests in Gradescope. You are welcome to create additional `make` rules, but we ask that
you keep the existing `final` and `checkpoint` rules, as those will be used by the autograders.


### Javadocs Documentation

Since there is a lot of potentially useful information in the comments within the starter code itself, it may also be
helpful to build the Javadocs documentation before diving into your design.  We have provide two additional `make` rules
to build the documentation for the main code packages (`naming`, `storage`, and `common`, most of which don't exist in 
the starter repo) and for the test suite packages (`test`, `test.util`, `test.naming`, `test.storage`, and `test.common`).
Running `make docs-test` will create a directory called `doc-test` that includes browseable documentation of the test
suite; just point a browser to `doc-test/index.html` and click around to see what's there.


### Questions?

If there is any part of the initial repository, environment setup, lab requirements, or anything else, please do not hesitate
to ask.  We're here to help!


### Appendix -- Summary of `make` Rules

| `make` rule | what it does | details |
|---|---|---|
| `build` | compiles all needed files | includes test suite and naming/storage directories if Java |
| `checkpoint` | run checkpoint tests in `test.Lab3CheckpointTests` | don't forget to update `test/ServerCommands.java` |
| `final` | run all tests in `test.Lab3FinalTests` | don't forget to update `test/ServerCommands.java` |
| `docs` | generate javadocs for naming/storage/common | browse via `doc/index.html` |
| `docs-test` | generate javadocs for test.naming/.storage/.common | browse via `doc-test/index.html` |
| `clean` | clean up the repo a bit | remove all .class files and javadocs directories |

