context("Select")

home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite(getOption('dplyr.calcite.model', NULL))

test_that("select can choose columns",{
  df <- select(tbl(src, "Master"), playerID, birthYear)
  vars <- tbl_vars(df)
  expect_equal(vars, c("playerID", "birthYear"))
})


test_that("select before group by",{
  df <- group_by(select(tbl(src, "Master"), playerID, birthYear), birthYear)
  vars <- tbl_vars(df)
  expect_equal(vars, c("playerID", "birthYear"))
})


test_that("select renames variables", {
  df <- select(tbl(src, "Master"), a=playerID, b=birthYear)
  vars <- tbl_vars(df)
  expect_equal(vars, c("a", "b"))
})


test_that("select raw SQL", {
  df <- select(tbl(src, sql('SELECT * FROM "Batting" WHERE "yearID" = 2008')), p=playerID, y=yearID)
  vars <- tbl_vars(df)
  expect_equal(vars, c("p", "y"))
})
