context("Count")

home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite(getOption('dplyr.calcite.model', NULL))
test_df <- tbl(src, "Batting")
s <- summarise(test_df,
          teams = count(distinct(teamID)),
          bats = n()
        )
s <- as.data.frame(s)

test_that("count gives the correct results on batting", {
  expect_equal(
    97889,
    nrow(test_df)
  )
})

test_that("count_distinct gives correct results for key types", {
  expect_equal(
    s$teams[1],
    length(unique(as.data.frame(test_df)$teamID))
  )
  expect_equal(
    s$bats[1],
    length(as.data.frame(test_df)$teamID)
  )
})

