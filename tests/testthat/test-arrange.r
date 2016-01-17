context("Arrange")

home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite(getOption('dplyr.calcite.model', NULL))

test_that("arrange orders correctly", {
  batting <- tbl(src, "Batting")
  t <- as.data.frame(head(arrange(batting, playerID, desc(yearID))))
  expect_equal(t$yearID, c(2013, 2012, 2010, 2009, 2008, 2007))
})
