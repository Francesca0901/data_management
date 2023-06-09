# movie_recommander
This is the individual project from EPFL's system for data management course.
a more detailed project discription can be found at: 
chrome-extension://hmigninkgibhdckiaphhmbgcghochdjc/pdfjs/web/viewer.html?file=https%3A%2F%2Fmoodle.epfl.ch%2Fpluginfile.php%2F3171624%2Fmod_resource%2Fcontent%2F4%2FCS460_2023_project-5.pdf

In this project, we are expected to implement data processing pipelines overApache Spark.
We are given a dataset based on the MovieLens datasets. 
The dataset includes: 
- A set of movies (e.g., movies, TV series) and their genres 
- A log of "user rates movie" actions 
A video streaming application requires large-scale data processing over the given dataset. The application has a user-facing component (which we are not concerned with) that serves recommendations and interesting statistics to the end-users. 
Specifically, for each title, it displays: 
- All the movies that are "similar" in terms of keywords
- An average rating 
In the background, the application issues Spark jobs that pre-compute the information that is served. Our task is to write Spark code for the required functionality.
