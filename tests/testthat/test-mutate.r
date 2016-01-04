context("Mutate")

home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite('../../data/model.json')

test_that("mutate some columns", {
  df <- mutate(select(tbl(src, "Master"), playerID, birthYear, weight), x=weight * 100)
  vars <- tbl_vars(df)
  expect_equal(vars, c("playerID", "birthYear", "weight", "x"))
})

