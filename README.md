##Base

This project is for shared functionality across components. Things one may consider part of this category:

* Spark utility functions
* Path prefixes
* CLI tool helpers

##Usage

In order to use in other projects, we need to ensure that the other you have the necessary leiningen plugin, [localrepo](https://github.com/kumarshantanu/lein-localrepo), and that this project exists on the lein classpath so that other projects can depend on it.

First, check if you have `localrepo` installed by running:
```
$ lein localrepo
```

If you see `'localrepo' is not a task. See 'lein help'.` in return it isn't installed.

While localrepo is a declared plugin in the majority of existing clojure component projects, and thus is accessible within those project directories, you may want to add it to your leiningen user profile. To do so you can refer to the `localrepo` github repository linked above.

Once you have it available running `lein localrepo` should return a help banner.

Before we finaally install this project as a local dependency, we need a fully packaged executable. To do so, generate an uberjar with:

```
lein uberjar
```

Finally, to install this project as a local dependency in your own components run the following in your project's directory:

```
$ lein localrepo install <path to standalone uberjar> com.puhrez.components/base <base version>
```
