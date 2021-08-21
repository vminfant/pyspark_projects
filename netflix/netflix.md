# Transform netflix data set

## Source

| Source        | Link           |
| ------------- |:-------------:|
| Kaggle        | [netflix-shows](https://www.kaggle.com/shivamb/netflix-shows) |

## Cleansing rules

* Remove duplicates based on ```show id```.
* Drop the programs/shows that are not ```TV Show``` or ```Movie```.
* Remove newline characters(\n) from ```title``` and ```description```.


## Transformation rules

* Convert the duration into seconds.

## Output
* Create two different output files based on type.

| Type          | Output File Delimiter  | Output File Format  | Output File Naming Convention |
| ------------- |:----------------------:|:-------------------:|:-----------------------------:|
| Movie         | Comma (,)              | CSV                 | netflix_movies_yyyymmdd.csv
| TV Show       | Comma (,)              | CSV                 | netflix_tv_shows_yyyymmdd.csv