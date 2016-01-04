# dplyr-calcite

dplyrcalcite is a Database connector for Calcite for dplyr the next iteration of plyr (from Hadley Wickham), focussed on tools for working with data frames (hence the `d` in the name).


## Installing dependencies

The interface to Calcite is driven by the Calcite JDBC driver.  This will require the driver to be installed, and one of the easiest ways to achieve this is by following the installation instructions provided here:
 http://calcite.incubator.apache.org/docs/howto.html
and:
 http://calcite.apache.org/docs/tutorial.html
With SQL reference here:
 http://calcite.apache.org/docs/reference.html

The examples provided in the data directory are dependent on the sample CSV file driver implementation.

## Install dependent R packages

install RJDBC and assertthat with:

* the latest released version from CRAN with

    ```R
    install.packages(c("RJDBC", "assertthat"))
    ````

next install lazyeval with:

* the latest released version from CRAN with

    ```R
    devtools::install_github("hadley/lazyeval")
    ````

next install dplyr with:

* the latest released version from CRAN with

    ```R
    install.packages("dplyr")
    ````

* the latest development version from github with

    ```R
    devtools::install_github("hadley/dplyr")
    ```

Finally install dplyr-calcite from github:

    ```R
    devtools::install_github("piersharding/dplyr-calcite")
    ```

To get started, read the notes below, then read the help(src_calcite).

If you encounter a clear bug, please file a minimal reproducible example on [github](https://github.com/piersharding/dplyr-calcite/issues).

## `src_calcite`

Connect to the Database:

```R
library(dplyrcalcite)

# optionally set the class path for the Calcite JDBC connector - alternatively use the CLASSPATH environment variable
options(dplyr.jdbc.classpath = "~/.m2/repository")

# To connect to a database first create a src:
lhm <- src_calcite('./data/model.json')
lhm

# Simple query:
batting <- tbl(lhm, "Batting")
dim(batting)
colnames(batting)
head(batting)
```

See dplyr for many more examples.
