context("Filter")

home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite('../../data/model.json')
t <- as.data.frame(tbl(src, sql('SELECT * FROM "Batting" WHERE "yearID" = 2008')))

test_that("filter results", {
  test_df <- as.data.frame(filter(tbl(src, "Batting"), yearID == 2008L))
  expect_equal(t, test_df)
})
