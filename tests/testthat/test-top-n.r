context("SQL: top_n")
home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite('../../data/model.json')

test_that("top_n returns n rows", {
  test_df <- tbl(src, "Batting")
  top_four <- test_df %>% top_nx(4)
  expect_equal(dim(top_four), c(5, 24))
})
