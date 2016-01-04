context("Summarise")

home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite('../../data/model.json')

test_that("apply mean", {
  test_df <- tbl(src, "Batting")
  out <- as.data.frame(summarise(test_df, x = mean(G_batting)))
  expect_equal(nrow(out), 1)
  expect_equal(ncol(out), 1)
  expect_equal(out$x, 48)
})

test_that("summarise peels off a single layer of grouping", {
  test_df <- tbl(src, "Batting")
  grouped <- group_by(test_df, teamID)
  summed <- as.data.frame(summarise(grouped, n=n()))
  expect_equal(nrow(summed), 149)
  expect_equal(ncol(summed), 2)
  expect_equal(summed$n[[1]], 25)
})
