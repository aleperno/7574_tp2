# Reddit Meme Analyzer

This program analyzes the datasets available at [kaggle](https://www.kaggle.com/datasets/pavellexyr/the-reddit-irl-dataset)
and outputs the following three results

 - Posts Score Average
 - Memes liked by students
 - Download meme with best average sentiment

Datasets should be located at the `datasets` directory, the default expected names are `complete-posts.csv`
and `complete-comments.csv`

The meme will be downloaded in the `results` directory

To run the program with docker simply run `make docker-compose-up`

The program allows the following customizations:

- Student Nodes Replicas: The nodes that calculate student memes can have multiple replicas
  this is done via the `STUDENT_MEME_CALCULATOR_REPLICAS` environment variable that is both
  located at `local.env` and the `docker-compose.yaml`

- Student Memes Repeat: The systems prints the URLs for the student memes. They might be
  duplicated. Allowing duplicates can be toggled by changing `STUDENT_MEME_ALLOW_REPEATS` in the
  docker compose

- Input files and chunks: For both the posts and comments we can configure where the program should
  be looking for the files and how many lines in a single chunk. Variables: `POSTS_FILE_PATH`,
  `POSTS_FILE_CHUNK`, `COMMENT_FILE_PATH`, `COMMENT_FILE_CHUNK`. In case of changing the paths, the
  volumes should also be updated.

- Meme Result: The path of where the meme will be downloaded and how many bytes per request are
  downloaded is also configurable `MEME_RESULT_PATH`, `MEME_RESULT_BUFFER`
