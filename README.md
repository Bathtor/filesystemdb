FileSystemDB
============
[![Build Status](https://travis-ci.org/Bathtor/filesystemdb.svg?branch=master)](https://travis-ci.org/Bathtor/filesystemdb)

Provides a simple, versioned key-value store, where every value(-version) is stored in a different file under a common folder.
This can be convenient for systems that have a key-value API, but allow (vastly) different value sizes, since normal key-value storage system don't tend to deal well with big values. The exact threshold might vary, but should be around 1MB for common systems.
