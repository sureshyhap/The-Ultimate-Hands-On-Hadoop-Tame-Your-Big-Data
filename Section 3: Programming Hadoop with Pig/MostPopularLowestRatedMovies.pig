ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:charArray, imdbLink:chararray);
    
nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group as movieID, AVG(ratings.rating) as avgRating, COUNT(ratings.rating) as numRating;

badMovies = FILTER avgRatings BY avgRating < 2.0;

badMoviesWithData = JOIN badMovies BY movieID, nameLookup by movieID;

mostPopularBadMovies = ORDER badMoviesWithData BY badMovies::numRating DESC;

final = FOREACH mostPopularBadMovies GENERATE nameLookup::movieTitle AS movieTitle, badMovies::avgRating AS avgRating, 
	badMovies::numRating AS numRating;

DUMP final;