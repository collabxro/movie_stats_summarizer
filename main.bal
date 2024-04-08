import ballerinax/mysql.driver as _;
import ballerinax/mysql;
import ballerina/io;

configurable string USER = ?;
configurable string PASSWORD = ?;
configurable string HOST = ?;
configurable int PORT = ?;
configurable string DATABASE = ?;

public type MovieCount record {|
    int year;
    int count;
|};

final mysql:Client dbClient = check new(host=HOST, user=USER, password=PASSWORD, port=PORT, database=DATABASE);

isolated function getMovieCountPerYear() returns MovieCount[]|error? {
    MovieCount[] movieCounts = [];
    stream<MovieCount, error?> resultStream = dbClient->query(
        `SELECT year ,COUNT(*) as count FROM movies GROUP BY year ORDER BY count DESC`
    );
    check from MovieCount movieCount in resultStream
        do {
            movieCounts.push(movieCount);
        };
    check resultStream.close();
    return movieCounts;
}

public function main() returns error? {
    MovieCount[]|error? ret = getMovieCountPerYear();
    if ret is error {
        return ret;
    }
    MovieCount[] movieCounts = <MovieCount[]> ret;
    foreach MovieCount movieCount in movieCounts {
        io:println("Year: " + movieCount.year.toString() + ", Movie Count: " + movieCount.count.toString());
    }
}