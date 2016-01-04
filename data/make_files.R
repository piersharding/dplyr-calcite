
if (1 == 2){

library(dplyr)

l <- src_postgres(dbname='lahman', user='postgres', password='password')

tables <- c(
'AllstarFull', 'Appearances', 'AwardsManagers', 'AwardsPlayers', 'AwardsShareManagers',
  'AwardsSharePlayers', 'Batting', 'BattingPost', 'Fielding', 'FieldingOF', 'FieldingPost', 'HallOfFame',
  'LahmanData', 'Managers', 'ManagersHalf', 'Master', 'Pitching', 'PitchingPost', 'Salaries', 'Schools',
  'SchoolsPlayers', 'SeriesPost', 'Teams', 'TeamsFranchises', 'TeamsHalf')

lapply(tables, function (x) { t <- as.data.frame(tbl(l, x));
                              cols <- sapply(t, class);
                              cols[cols == 'integer'] <- ':int';
                              cols[cols == 'numeric'] <- ':float';
                              cols[cols == 'character'] <- '';
                              names(cols) <- names(t);
                              t[is.na(t)] <- 0;
                              f <- paste0('./tables/',x,'.csv');
                              cat(paste(lapply(names(t), function(y){paste0(y, cols[[y]])}), collapse=","), "\n", file=f, append=FALSE);
                              write.table(t, file=f, sep=",", quote=TRUE, append=TRUE, col.names=FALSE, row.names=FALSE, na="0")})

}




