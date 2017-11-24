# SI 507 F17 - Project 6 - Databases & Planning

### Database name: koos_507project6

### DEADLINE: Monday, November 27th at 11:59 PM

## To Submit

* A link to your fork of this GitHub repository, having committed all the files you've changed and added. See below for further instructions.

* A link to the GitHub repository you've started for your final project, with the milestones and issues you've created for your final project plan. See below!


### Note:

* *Do not* try to submit your Postgres database itself. You should make sure the database you end up using for this project is called **YOUR_UNIQNAME_507project6** (so mine would be `jczetta_507project6` or Anand's would be `anandpd_507project6`) and that you have added that name to this README file in your fork, as directed below. We will be running your code to recreate the database on our own computer(s).


## Instructions

## Part 1 (600 points) - Creating & adding data to a database

We've provided 3 CSV files that should be familiar -- they're the CSV files you should have produced something similar to in your Project 3, when you scraped NPS.gov:

* `arkansas.csv`
* `michigan.csv`
* `california.csv`

Each CSV file contains data about National Sites (parks, historical sites, lakeshores, etc) registered with the National Park Service.

In Project 3, you gathered this data by scraping the NPS website. Now, armed with these CSV files, you will be creating tables and adding this data to database tables.

First, you should create a database. You can try with an "interim" database if you like, but ultimately, the database for this project should be called **YOUR_UNIQNAME_507project6** (so mine would be `jczetta_507project6` or Anand's would be `anandpd_507project6`).

*You should edit the README file -- this file -- in your fork, at the very top, and write your database name.* That will make it easy for us to copy and paste it to create the databases we need to create. This is worth points, because it's important for our grading process!

The remainder of Part 1 and Part 2 should be written in a code file called `SI507_project6.py` (included here, without any content).

*You should create 2 database tables, with the following columns in each:*

* **Sites**

    * ID (SERIAL)
    * Name (VARCHAR up to 128 chars, UNIQUE)
    * Type [e.g. "National Lakeshore" or "National Park"] (VARCHAR up to 128 chars)
    * State_ID (INTEGER - FOREIGN KEY REFERENCING States)
    * Location (VARCHAR up to 255 chars)
    * Description (TEXT)

* **States**

    * ID (SERIAL)
    * Name (VARCHAR up to 40 chars, UNIQUE)


And you should add the data from those `.CSV` files to those database tables as appropriate.

HINT: There should be only 3 rows in the **States** table when you're done inserting data, but a LOT of rows in the **Sites** table!

Examples of code you've seen in class will be very useful here.

**Note:** it's a good idea to plan out how your code process will go before starting to write your code.

**Note also** that the way the data is structured in the .CSV files you created is not the same as the way you want to structure it in the database! Because of the power of relational databases, it's not a good idea to have a table `National Sites in Michigan` and another `National Sites in California`, etc. So you'll be using the data you got before, and restructuring it.

We have provided sample CSV files so everyone is working from the same files.

## Part 2 (200 points) - Making queries to a database

* In Python, query the database for all of the **locations** of the sites. (Of course, this data may vary from "Detroit, Michigan" to "Various States: AL, AK, AR, OH, CA, NV, MD" or the like. That's OK!) Save the resulting data in a variable called `all_locations`.

* In Python, query the database for all of the **names** of the sites whose **descriptions** include the word `beautiful`. Save the resulting data in a variable called `beautiful_sites`.

* In Python, query the database for the total number of **sites whose type is `National Lakeshore`.** Save the resulting data in a variable called `natl_lakeshores`.

* In Python, query your database for the **names of all the national sites in Michigan**. Save the resulting data in a variable called `michigan_names`. You should use an inner join query to do this.

* In Python, query your database for the **total number of sites in Arkansas**. Save the resulting data in a variable called `total_number_arkansas`. You can use multiple queries + Python code to do this, one subquery, or one inner join query. HINT: You'll need to use an aggregate function!

## Part 3 (400 points, and feedback) - Final Project Milestones

[Here](https://paper.dropbox.com/doc/SI-507-Fall-2017-Final-Project-XwIGPUCZrTaBNTT75uU4B) are your final project requirements. We'll be updating them with increased clarity (e.g. how many points go with what) over the next week or so.

For your Part 3, you should

* Create a GitHub repository for your final project. You don't need to create a git repository with code on your computer yet -- though eventually you'll need to, and you'll need to "build a bridge" between that git repository and your GitHub repository online.

* In that GitHub repository, online, create milestones and issues to lay out the process of your final project, the way you did in [section](https://github.com/SI507-F17/section-week-5) on a few occasions.

* We'll grade this on whether you tried it, but we'll also offer feedback about whether your final project sounds doable/sounds like it's scoped reasonably/if your plan sounds solid/if we have brief recommendations.
