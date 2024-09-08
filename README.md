# MiniDB
## Project Description
MiniDB is a light-weight database management system. It consists of:
- Classes that represent fields, tuples, and tuple schemas;

- Classes that apply predicates and conditions to tuples;

- Access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples
  of those relations;

- A collection of operator classes to process tuples: select, join, insert, delete, etc.

- A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions;

- A catalog that stores information about available tables and their schemas.
  
MiniDB is a library-based project which focuses on building a robust static library called **db**.
This library is designed to provide core database functionalities to applications with high performance. It
is an ideal choice for projects requiring embedded or customizable database library.  

## How to run this program?
I recommend to use CMake and Unix Makefile generator to compile and run the program. For IDE, I recommend CLion
or VS Code. Note that CLion uses ninja MakeFile generator which may conflicts with Unix Makefile generated files.
Here are the steps to build in the terminal:
1. Git clone the repo.
2. Navigate to downloaded folder directory.
   ```shell
    cd YourWorkSpaceDir/MiniDB
    ```
3. Compile.
    ```cmake
    cmake --build .
    ```
4. Test. Navigate to your build directory, here it is miniDB/cmake-build-debug/. Then run tests using CTest. 
    ```shell
    cd cmake-build-debug
    ctest
    ```

5. Debug.
   If you encountered build errors and want to clean your build directory and rebuild the program, try these steps:
    ```shell
   rm -rf cmake-build-debug
   mkdir cmake-build-debug
   cd cmake-build-debug
   cmake -DCMAKE_BUILD_TYPE=Debug ..
   cmake --build .
   ```
