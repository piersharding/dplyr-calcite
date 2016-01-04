context("Group by")

home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite('../../data/model.json')

test_that("groups do something", {
  batting <- tbl(src, "Batting")
  g <- group_by(batting, playerID, yearID) %>% summarise_each(funs(mean), matches("stint"))
  vars <- tbl_vars(g)
  expect_equal(vars, c("playerID", "yearID", "stint"))
  expect_equal(90802, nrow(g))
})

