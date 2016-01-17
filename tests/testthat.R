library("testthat")
library("dplyrcalcite")

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
    home <- Sys.getenv("HOME", "~/.m2/repository")
    options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
    options(dplyr.show_sql = TRUE)
    options(dplyr.calcite.model = paste0(getwd(), '/../../data/model.json'))
    print(getOption('dplyr.calcite.model', NULL))
    test_check("dplyrcalcite")
    devtools::test()
}

