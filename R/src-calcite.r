#' Connect to Calcite (http://calcite.incubator.apache.org/), an Open Source standardised SQL query interface
#'
#' Use \code{src_calcite} to connect to an existing Calcite database,
#' and \code{tbl} to connect to tables within that database. Please note that the ORDER BY, LIMIT and OFFSET keywords
#' are not supported in the query when using \code{tbl} on a connection to a Calcite database.
#' If you are running a local database, you only need to define the name of the database you want to connect to.
#' Calcite does nto support anti-joins.
#'
#' @template db-info
#' @param dbname Database name
#' @param host,port Host name and port number of database (defaults to localhost:21050)
#' @param user,password User name and password (if needed)
#' @param opts=list() a list of options passed to the Calcite driver - defaults to opts=list(auth="noSasl")
#' @param ... for the src, other arguments passed on to the underlying
#'   database connector, \code{dbConnect}.
#' @param src a Calcite src created with \code{src_calcite}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link{sql}} described a derived table or compound join.
#' @export
#' @examples
#' \dontrun{
#' # Connection basics ---------------------------------------------------------
#' # optionally set the class path for the Calcite JDBC connector - alternatively use the CLASSPATH
#' # environment variable
#' options(dplyr.jdbc.classpath = '/path/to/calcite-jdbc')
#' # To connect to a database first create a src:
#' my_db <- src_calcite(dbname="demo")
#' # Then reference a tbl within that src
#' my_tbl <- tbl(my_db, "my_table")
#' }
#'
#' # Here we'll use the Lahman database: to create your own local copy,
#' # create a local database called "lahman" first.
#'
#' if (has_lahman("calcite")) {
#' # Methods -------------------------------------------------------------------
#' batting <- tbl(lahman_calcite(), "Batting")
#' dim(batting)
#' colnames(batting)
#' head(batting)
#'
#' # Data manipulation verbs ---------------------------------------------------
#' filter(batting, yearID > 2005, G > 130)
#' select(batting, playerID:lgID)
#' arrange(batting, playerID, desc(yearID))
#' summarise(batting, G = mean(G), n = n())
#' mutate(batting, rbi2 = if(is.null(AB)) 1.0 * R / AB else 0)
#'
#' # note that all operations are lazy: they don't do anything until you
#' # request the data, either by `print()`ing it (which shows the first ten
#' # rows), by looking at the `head()`, or `collect()` the results locally.
#'
#' system.time(recent <- filter(batting, yearID > 2010))
#' system.time(collect(recent))
#'
#' # Group by operations -------------------------------------------------------
#' # To perform operations by group, create a grouped object with group_by
#' players <- group_by(batting, playerID)
#' group_size(players)
#' summarise(players, mean_g = mean(G), best_ab = max(AB))
#'
#' # When you group by multiple level, each summarise peels off one level
#' per_year <- group_by(batting, playerID, yearID)
#' stints <- summarise(per_year, stints = max(stint))
#' filter(stints, stints > 3)
#' summarise(stints, max(stints))
#'
#' # Joins ---------------------------------------------------------------------
#' player_info <- select(tbl(lahman_calcite(), "Master"), playerID, hofID,
#'   birthYear)
#' hof <- select(filter(tbl(lahman_calcite(), "HallOfFame"), inducted == "Y"),
#'  hofID, votedBy, category)
#'
#' # Match players and their hall of fame data
#' inner_join(player_info, hof)
#' # Keep all players, match hof data where available
#' left_join(player_info, hof)
#' # Find only players in hof
#' semi_join(player_info, hof)
#' # Calcite cannot do anti-joins
#'
#' # Arbitrary SQL -------------------------------------------------------------
#' # You can also provide sql as is, using the sql function:
#' batting2008 <- tbl(lahman_calcite(),
#'   sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
#' batting2008
#' }


expandAndCheckClassPath <- function(classpath=NULL,
         driverclass="org.apache.calcite.jdbc.Driver", jarfiles=c("calcite-avatica-1.6.0-SNAPSHOT.jar",
                                                                  # "calcite-avatica-server-.*-SNAPSHOT.jar",
                                                                  "calcite-linq4j-.*-SNAPSHOT.jar",
                                                                  "calcite-core-.*-SNAPSHOT.jar",
                                                                  "jackson-annotations-2.1.1.jar",
                                                                  "eigenbase-properties-1.1.5.jar",
                                                                  "janino-2.7.6.jar",
                                                                  "commons-compiler-2.7.6.jar",
                                                                  "jackson-core-2.1.1.jar",
                                                                  "jackson-databind-2.1.1.jar",
                                                                  "guava-14.0.1.jar",


                                                                  # CSV libs
                                                                  "calcite-example-csv-.*-SNAPSHOT.jar",
                                                                  "opencsv-.*.jar",
                                                                  "commons-lang3-.*.jar",
                                                                  # "calcite-mongodb-.*-SNAPSHOT.jar",
                                                                  # "calcite-plus-.*-SNAPSHOT.jar",
                                                                  # "calcite-spark-.*-SNAPSHOT.jar",
                                                                  # "calcite-splunk-.*-SNAPSHOT.jar",
                                                                  # "calcite-ubenchmark-.*-SNAPSHOT.jar"
                                                                  "commons-logging.*.jar",
                                                                #    "hadoop-common.jar",
                                                                #    "hive-jdbc.*.jar",
                                                                #    "hive-common.*.jar",
                                                                #    "hive-metastore.*.jar",
                                                                #    "hive-service.*.jar",
                                                                #    "libfb303.*.jar",
                                                                #    "libthrift.*.jar",
                                                                #    "commons-httpclient.*.jar",
                                                                #    "httpclient.*.jar",
                                                                #    "httpcore.*.jar",
                                                                   "log4j.*.jar",
                                                                   "slf4j-api.*.jar",
                                                                   "slf4j-log4j.*.jar"
                                                                #    "hive-exec.jar"
                                                                   )) {

  if (is.null(classpath)) classpath <- getOption('dplyr.jdbc.classpath', NULL)
  if (is.null(classpath)) classpath <- unname(Sys.getenv("CLASSPATH"))
  classpath <- unlist(strsplit(classpath, ":"))

  jar.search.path <- c(classpath,
                       # ".",
                       # Sys.getenv("CLASSPATH"),
                       # Sys.getenv("PATH"),
                       if (.Platform$OS == "windows") {
                         file.path(Sys.getenv("PROGRAMFILES"), "Calcite")
                       } else c("/usr/lib/calcite", "/usr/lib/calcite/lib"))

  classpath <- lapply(jarfiles,
                      function (x) { head(list.files(path=list.files(path=jar.search.path, full.names=TRUE, all.files=TRUE),
                                                     pattern=paste0("^",x,"$"), full.names=TRUE, recursive=TRUE), 1)})
  x <- do.call(paste, c(as.list(classpath), sep=":"))
  # print(x)
  x
}


src_calcite <- function(model, user = "", password = "", opts=list(), ...) {

  if (!require("dplyr")) {
    stop("dplyr package required to connect to Calcite", call. = FALSE)
  }

  if (!require("RJDBC")) {
    stop("RJDBC package required to connect to Calcite", call. = FALSE)
  }

  driverclass <- "org.apache.calcite.jdbc.Driver"
  if (length(names(opts)) > 0) {
    opts <- paste0(";", paste(lapply(names(opts), function(x){paste(x,opts[x], sep="=")}), collapse=";"))
  }
  else {
    opts <- ""
  }

  url <- paste0("jdbc:calcite:model=", model, opts)
  # print(paste0("model is: ", url))
  con <- dbConnect(JDBC(driverclass,
            expandAndCheckClassPath(driverclass=driverclass),
            identifier.quote='"'), url, ...)

  # res <- dbGetQuery(con, 'SELECT version() AS version') # do this instead of dbGetInfo(con) - returns nothing because of RJDBC!
  res <- dbGetInfo(con)
  md <- .jcall(con@jc, "Ljava/sql/DatabaseMetaData;", "getMetaData")
  vers <- paste0("Product: ",
                 .jcall(md, "Ljava/lang/String;", "getDatabaseProductName"),
                 "/",
                 .jcall(md, "Ljava/lang/String;", "getDatabaseProductVersion"),
                 " - Driver: ", .jcall(md, "Ljava/lang/String;", "getDriverName"),
                 "/",
                 .jcall(md, "Ljava/lang/String;", "getDriverVersion"))
  info <- list(dbname=model, url=url, version=vers)

  # stash the dbname on the connection for things like db_list_tables
  attr(con, 'dbname') <- model

  # I don't trust the connector - so switch the database
  # qry_run_once(con, paste("USE ", dbname))

  env <- environment()
  # temporarily suppress the warning messages from getPackageName()
  wmsg <- getOption('warn')
  options(warn=-1)
  # bless JDBCConnection into ImaplaDB Connection class - this way we can give the JDBC connection
  # driver specific powers
  CalciteConnection <- methods::setRefClass("CalciteConnection", contains = c("JDBCConnection"), where = env)
  options(warn=wmsg)
  con <- structure(con, class = c("CalciteConnection", "JDBCConnection"))

  # Creates an environment that disconnects the database when it's
  # garbage collected
  db_disconnector <- function(con, name, quiet = FALSE) {
    reg.finalizer(environment(), function(...) {
      if (!quiet) {
        message("Auto-disconnecting ", name, " connection")
      }
      dbDisconnect(con)
    })
    environment()
  }

  src_sql("calcite", con,
    info = info, disco = db_disconnector(con, "calcite", ))
}

#' @export
src_desc.src_calcite <- function(x) {
  info <- x$info
  paste0("Calcite ", "serverVersion: ", info$version, " [",  info$url, "]\n")
}

#' @export
#' @rdname src_calcite
tbl.src_calcite <- function(src, from, ...) {
  tbl_sql("calcite", src = src, from = from, ...)
}

#' @export
src_translate_env.src_calcite <- function(x) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
      n = function() sql("COUNT(*)"),
      sd =  sql_prefix("STDDEV_SAMP"),
      var = sql_prefix("VAR_SAMP"),
      median = sql_prefix("MEDIAN"),
      min_rank = sql_prefix("RANK"),
      max_rank = sql_prefix("RANK")
      # rank = sql_prefix("RANK()"),
      # top_n = sql_prefix(paste0('RANK() OVER(ORDER BY "', wt, '" DESC) AS top_n'))
    )
  )
}
# src_translate_env.src_calcite <- function(x) {
#   z <- sql_variant(
#     base_scalar,
#     sql_translator(.parent = base_agg,
#       n = function() sql("COUNT(*)"),
#       sd =  sql_prefix("STDDEV_SAMP"),
#       var = sql_prefix("VAR_SAMP"),
#       median = sql_prefix("MEDIAN")
#     )
#   )
#   print(z)
#   z
# }


#' @export
db_has_table.CalciteConnection <- function(con, table) {
  table %in% db_list_tables(con)
}

#' @export
db_list_tables.CalciteConnection <- function(con) {
   tables <- dbListTables(con)
   tables <- subset(tables, !(tables %in% c('COLUMNS', 'TABLES')))
   tables
}

#' @export
db_data_type.CalciteConnection <- function(con, fields) {

  data_type <- function(x) {
    switch(class(x)[1],
      logical = "boolean",
      integer = "integer",
      numeric = "double",
      factor =  "STRING",
      character = "STRING",
      Date =    "TIMESTAMP",
      POSIXct = "TIMESTAMP",
      stop("Unknown class ", paste(class(x), collapse = "/"), call. = FALSE)
    )
  }
  vapply(fields, data_type, character(1))
}


copy_to.src_calcite <- function(dest, df, name = deparse(substitute(df)),
                            types = NULL, temporary = FALSE, indexes = NULL,
                            analyze = TRUE, ...) {
  # Calcite can't create temporary tables

  assert_that(is.data.frame(df), is.string(name), is.flag(temporary))
  if (isTRUE(db_has_table(dest$con, name))) {
    stop("Table ", name, " already exists.", call. = FALSE)
  }

  types <- types %||% db_data_type(dest$con, df)
  names(types) <- names(df)

  con <- dest$con

  db_begin(con)
  sql_create_table(con, name, types, temporary = temporary)
  db_insert_into(con, name, df)
  if (analyze) db_analyze(con, name)
  db_commit(con)
  tbl(dest, name)
}


#' @export
db_query_fields.CalciteConnection <- function(con, from) {
  # doesn't like the ; on the end
  # print(paste0("QRY FIELDS: ", from))
  qry <- dbGetQuery(con, build_sql("SELECT * FROM ", from, " WHERE 0=1", con = con))
  names(qry)
}


double_escape <- function(x) {
  structure(x, class = c("sql", "sql", "character"))
}


#' @export
dim.tbl_calcite <- function(x) {
  n <- x$query$nrow()

  p <- x$query$ncol()
  c(n, p)
}


top_nx <- function(x, n, wt) {
  if (missing(wt)) {
    vars <- tbl_vars(x)
    message("Selecting by ", vars[length(vars)])
    wt <- as.name(vars[length(vars)])
  } else {
    wt <- substitute(wt)
  }

  stopifnot(is.numeric(n), length(n) == 1)
  if (n < 0) {
    n <- abs(n)
  }

  pieces <- mapply(function(fld) {
      return(sql_quote(fld, '"'))
    }, as.character(x$select))

  fields <- do.call(function(...) {paste(..., sep=", ")}, as.list(pieces))
  fields <- double_escape(fields)
  fields <- paste0(fields, ', RANK() OVER(ORDER BY "', wt, '" DESC) AS "top_n"')
  select <- paste0("SELECT ", do.call(function(...) {paste(..., sep=", ")}, as.list(pieces)))
  from <- paste0(' FROM (SELECT ', fields, ' FROM ', sql_quote(x$from,'"'), ' ',
    x$where,') ')
  where <- paste0(' WHERE ',  sql_quote('top_n','"'), ' <= ', n)
  sql <- paste0(select, from, where)
  # print(paste0("SQL: ", sql))

  return(tbl(x$src, sql(sql)))
}




lahman_calcite <- function(...) stop("Calcite can't do this!")
