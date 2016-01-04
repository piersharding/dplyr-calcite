context("Joins")

home <- Sys.getenv("HOME", "~/.m2/repository")
options(dplyr.jdbc.classpath =  paste0(home, "/.m2/repository"))
src <- src_calcite('../../data/model.json')

test_that("inner join", {
  player_info <- select(tbl(src, "Master"), playerID, birthYear)
  hof <- select(filter(tbl(src, "HallOfFame"), inducted == "Y"), playerID, votedBy, category)
  t <- inner_join(player_info, hof)
  vars <- tbl_vars(t)
  expect_equal(vars, c("playerID", "birthYear", "playerID0", "votedBy", "category"))
  expect_equal(306, nrow(t))
})


test_that("left join", {
  player_info <- select(tbl(src, "Master"), playerID, birthYear)
  hof <- select(filter(tbl(src, "HallOfFame"), inducted == "Y"), playerID, votedBy, category)
  t <- left_join(player_info, hof)
  vars <- tbl_vars(t)
  expect_equal(vars, c("playerID", "birthYear", "playerID0", "votedBy", "category"))
  expect_equal(18354, nrow(t))
})


test_that("semi join", {
  player_info <- select(tbl(src, "Master"), playerID, birthYear)
  hof <- select(filter(tbl(src, "HallOfFame"), inducted == "Y"), playerID, votedBy, category)
  t <- semi_join(player_info, hof)
  vars <- tbl_vars(t)
  expect_equal(vars, c("playerID", "birthYear"))
  expect_equal(306, nrow(t))
})


test_that("anti join", {
  player_info <- select(tbl(src, "Master"), playerID, birthYear)
  hof <- select(filter(tbl(src, "HallOfFame"), inducted == "Y"), playerID, votedBy, category)
  t <- anti_join(player_info, hof)
  vars <- tbl_vars(t)
  expect_equal(vars, c("playerID", "birthYear"))
  expect_equal(18048, nrow(t))
})

